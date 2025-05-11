"""
Schema Manager for DSPy Text-to-SQL
This module handles database schema loading, processing, and formatting.
"""

import os
import sqlite3
import logging
from typing import List, Dict, Any

# Import from original codebase
from core.utils import load_json_file, is_email, is_valid_date_column

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SchemaManager:
    """
    Class to manage database schema information.
    Handles loading and processing schema information from database files.
    """
    
    def __init__(self, data_path, tables_json_path):
        self.data_path = data_path
        self.tables_json_path = tables_json_path
        self.db2infos = {}  # Cache for database info
        self.db2dbjsons = {}  # Cache for table JSON data
        
        # Load all database JSON information
        self.init_db2jsons()
    
    def init_db2jsons(self):
        """Initialize database metadata from tables.json"""
        if not os.path.exists(self.tables_json_path):
            raise FileNotFoundError(f"tables.json not found in {self.tables_json_path}")
        
        data = load_json_file(self.tables_json_path)
        for item in data:
            db_id = item['db_id']
            
            table_names = item['table_names']
            item['table_count'] = len(table_names)
            
            column_count_lst = [0] * len(table_names)
            for tb_idx, col in item['column_names']:
                if tb_idx >= 0:
                    column_count_lst[tb_idx] += 1
            
            item['max_column_count'] = max(column_count_lst)
            item['total_column_count'] = sum(column_count_lst)
            item['avg_column_count'] = sum(column_count_lst) // len(table_names)
            
            self.db2dbjsons[db_id] = item
    
    def get_db_schema(self, db_id, extracted_schema=None, use_gold_schema=False):
        """Get database schema description string"""
        if db_id not in self.db2infos:
            self.db2infos[db_id] = self._load_single_db_info(db_id)
        
        db_info = self.db2infos[db_id]
        
        # Build schema description and foreign key info
        schema_str, fk_str, chosen_columns = self._build_schema_description(
            db_id, db_info, extracted_schema or {}, use_gold_schema
        )
        
        return {
            "schema_str": schema_str,
            "fk_str": fk_str,
            "chosen_columns": chosen_columns
        }
    
    def _get_column_attributes(self, cursor, table):
        """Get column attributes from the database"""
        cursor.execute(f"PRAGMA table_info(`{table}`)")
        columns = cursor.fetchall()
        
        column_names = []
        column_types = []
        for column in columns:
            column_names.append(column[1])
            column_types.append(column[2])
        
        return column_names, column_types
    
    def _get_unique_column_values_str(self, cursor, table, column_names, column_types, 
                                     json_column_names, is_key_column_lst):
        """Get unique column values as strings for representation"""
        col_to_values_str_lst = []
        col_to_values_str_dict = {}
        
        key_col_list = [json_column_names[i] for i, flag in enumerate(is_key_column_lst) if flag]
        
        for idx, column_name in enumerate(column_names):
            # Skip primary and foreign keys
            if column_name in key_col_list:
                continue
                
            lower_column_name = column_name.lower()
            # Skip ID, email, and URL columns
            if (lower_column_name.endswith('id') or 
                lower_column_name.endswith('email') or 
                lower_column_name.endswith('url')):
                col_to_values_str_dict[column_name] = ''
                continue
                
            # Get unique values for this column
            sql = f"SELECT `{column_name}` FROM `{table}` GROUP BY `{column_name}` ORDER BY COUNT(*) DESC"
            cursor.execute(sql)
            values = cursor.fetchall()
            values = [value[0] for value in values]
            
            # Process values to get a representative string
            values_str = ''
            try:
                values_str = self._get_value_examples_str(values, column_types[idx])
            except Exception as e:
                logger.error(f"Error getting example values: {e}")
            
            col_to_values_str_dict[column_name] = values_str
        
        # Organize values for all columns
        for k, column_name in enumerate(json_column_names):
            values_str = ''
            is_key = is_key_column_lst[k]
            
            if is_key:
                values_str = ''
            elif column_name in col_to_values_str_dict:
                values_str = col_to_values_str_dict[column_name]
            else:
                logger.warning(f"Column name {column_name} not found in values dictionary")
            
            col_to_values_str_lst.append([column_name, values_str])
        
        return col_to_values_str_lst
    
    def _get_value_examples_str(self, values, col_type):
        """Format value examples for display"""
        if not values:
            return ''
            
        # Skip detailed examples for numeric columns with many values
        if len(values) > 10 and col_type in ['INTEGER', 'REAL', 'NUMERIC', 'FLOAT', 'INT']:
            return ''
            
        vals = []
        has_null = False
        
        # Process values
        for v in values:
            if v is None:
                has_null = True
            else:
                tmp_v = str(v).strip()
                if tmp_v != '':
                    vals.append(v)
                    
        if not vals:
            return ''
            
        # Handle text columns specially
        if col_type in ['TEXT', 'VARCHAR']:
            new_values = []
            
            for v in vals:
                if not isinstance(v, str):
                    new_values.append(v)
                else:
                    v = v.strip()
                    if v == '':
                        continue
                    elif ('https://' in v) or ('http://' in v):
                        return ''
                    elif is_email(v):
                        return ''
                    else:
                        new_values.append(v)
                        
            vals = new_values
            tmp_vals = [len(str(a)) for a in vals]
            if not tmp_vals:
                return ''
            max_len = max(tmp_vals)
            if max_len > 50:
                return ''
        
        if not vals:
            return ''
            
        # Limit to first 6 values
        vals = vals[:6]
        
        # Special handling for date columns
        is_date_column = is_valid_date_column(vals)
        if is_date_column:
            vals = vals[:1]
            
        if has_null:
            vals.insert(0, None)
            
        return str(vals)
    
    def _load_single_db_info(self, db_id):
        """Load schema information for a single database"""
        table2coldescription = {}  # Column descriptions
        table2primary_keys = {}    # Primary keys
        table_foreign_keys = {}    # Foreign keys
        table_unique_column_values = {}  # Sample values
        
        db_dict = self.db2dbjsons[db_id]
        
        # Collect all primary and foreign keys
        important_key_id_lst = []
        keys = db_dict['primary_keys'] + db_dict['foreign_keys']
        for col_id in keys:
            if isinstance(col_id, list):
                important_key_id_lst.extend(col_id)
            else:
                important_key_id_lst.append(col_id)
        
        # Connect to the database
        db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        
        # Process each table
        table_names_original_lst = db_dict['table_names_original']
        all_column_names_original_lst = db_dict['column_names_original']
        all_column_names_full_lst = db_dict['column_names']
        
        for tb_idx, tb_name in enumerate(table_names_original_lst):
            col2dec_lst = []
            pure_column_names_original_lst = []
            is_key_column_lst = []
            
            # Process columns for this table
            for col_idx, (root_tb_idx, orig_col_name) in enumerate(all_column_names_original_lst):
                if root_tb_idx != tb_idx:
                    continue
                    
                pure_column_names_original_lst.append(orig_col_name)
                is_key_column_lst.append(col_idx in important_key_id_lst)
                
                full_col_name = all_column_names_full_lst[col_idx][1]
                full_col_name = full_col_name.replace('_', ' ')
                
                col2dec_lst.append([orig_col_name, full_col_name, ''])
                
            table2coldescription[tb_name] = col2dec_lst
            table_foreign_keys[tb_name] = []
            table_unique_column_values[tb_name] = []
            table2primary_keys[tb_name] = []
            
            # Get column attributes and sample values
            all_sqlite_column_names_lst, all_sqlite_column_types_lst = self._get_column_attributes(cursor, tb_name)
            col_to_values_str_lst = self._get_unique_column_values_str(
                cursor, tb_name, 
                all_sqlite_column_names_lst, 
                all_sqlite_column_types_lst,
                pure_column_names_original_lst, 
                is_key_column_lst
            )
            table_unique_column_values[tb_name] = col_to_values_str_lst
        
        # Process foreign keys
        foreign_keys_lst = db_dict['foreign_keys']
        for from_col_idx, to_col_idx in foreign_keys_lst:
            from_col_name = all_column_names_original_lst[from_col_idx][1]
            from_tb_idx = all_column_names_original_lst[from_col_idx][0]
            from_tb_name = table_names_original_lst[from_tb_idx]
            
            to_col_name = all_column_names_original_lst[to_col_idx][1]
            to_tb_idx = all_column_names_original_lst[to_col_idx][0]
            to_tb_name = table_names_original_lst[to_tb_idx]
            
            table_foreign_keys[from_tb_name].append((from_col_name, to_tb_name, to_col_name))
        
        # Process primary keys
        for pk_idx in db_dict['primary_keys']:
            pk_idx_lst = []
            if isinstance(pk_idx, int):
                pk_idx_lst.append(pk_idx)
            elif isinstance(pk_idx, list):
                pk_idx_lst = pk_idx
            else:
                raise ValueError(f"Unexpected primary key format: {pk_idx}")
                
            for cur_pk_idx in pk_idx_lst:
                tb_idx = all_column_names_original_lst[cur_pk_idx][0]
                col_name = all_column_names_original_lst[cur_pk_idx][1]
                tb_name = table_names_original_lst[tb_idx]
                table2primary_keys[tb_name].append(col_name)
        
        cursor.close()
        conn.close()
        
        # Return combined schema info
        return {
            "desc_dict": table2coldescription,
            "value_dict": table_unique_column_values,
            "pk_dict": table2primary_keys,
            "fk_dict": table_foreign_keys
        }
    
    def _build_schema_description(self, db_id, db_info, extracted_schema, use_gold_schema):
        """Build the schema description string from database info"""
        desc_info = db_info['desc_dict']
        value_info = db_info['value_dict']
        pk_info = db_info['pk_dict']
        fk_info = db_info['fk_dict']
        
        schema_desc_str = ''
        db_fk_infos = []
        
        # Collect chosen columns for tracking
        chosen_db_schem_dict = {}
        
        # Process each table
        for (table_name, columns_desc), (_, columns_val), (_, table_fks), (_, table_pks) in zip(
            desc_info.items(), value_info.items(), fk_info.items(), pk_info.items()
        ):
            table_decision = extracted_schema.get(table_name, '')
            if table_decision == '' and use_gold_schema:
                continue
                
            all_columns = [name for name, _, _ in columns_desc]
            primary_key_columns = [name for name in table_pks]
            foreign_key_columns = [name for name, _, _ in table_fks]
            important_keys = primary_key_columns + foreign_key_columns
            
            # Apply filtering based on selector's decision
            new_columns_desc = []
            new_columns_val = []
            
            if table_decision == "drop_all":
                # Include only first 6 columns
                new_columns_desc = columns_desc[:6].copy()
                new_columns_val = columns_val[:6].copy()
            elif table_decision == "keep_all" or table_decision == '':
                # Include all columns
                new_columns_desc = columns_desc.copy()
                new_columns_val = columns_val.copy()
            else:
                # Include specified columns plus important keys
                llm_chosen_columns = table_decision
                append_col_names = []
                
                # First add key columns
                for idx, col in enumerate(all_columns):
                    if col in important_keys:
                        new_columns_desc.append(columns_desc[idx])
                        new_columns_val.append(columns_val[idx])
                        append_col_names.append(col)
                
                # Then add selected columns
                for idx, col in enumerate(all_columns):
                    if col in llm_chosen_columns and col not in append_col_names:
                        new_columns_desc.append(columns_desc[idx])
                        new_columns_val.append(columns_val[idx])
                        append_col_names.append(col)
                
                # Ensure we have at least 6 columns if available
                if len(all_columns) > 6 and len(new_columns_val) < 6:
                    for idx, col in enumerate(all_columns):
                        if len(append_col_names) >= 6:
                            break
                        if col not in append_col_names:
                            new_columns_desc.append(columns_desc[idx])
                            new_columns_val.append(columns_val[idx])
                            append_col_names.append(col)
            
            # Save selected columns
            chosen_db_schem_dict[table_name] = [col_name for col_name, _, _ in new_columns_desc]
            
            # Build schema string in list format
            schema_desc_str += f"# Table: {table_name}\n"
            extracted_column_infos = []
            
            for (col_name, full_col_name, col_extra_desc), (_, col_values_str) in zip(new_columns_desc, new_columns_val):
                col_extra_desc = 'And ' + str(col_extra_desc) if col_extra_desc != '' and str(col_extra_desc) != 'nan' else ''
                col_extra_desc = col_extra_desc[:100]
                
                col_line_text = f'  ({col_name},'
                
                if full_col_name != '':
                    full_col_name = full_col_name.strip()
                    col_line_text += f" {full_col_name}."
                if col_values_str != '':
                    col_line_text += f" Value examples: {col_values_str}."
                if col_extra_desc != '':
                    col_line_text += f" {col_extra_desc}"
                    
                col_line_text += '),'
                extracted_column_infos.append(col_line_text)
                
            schema_desc_str += '[\n' + '\n'.join(extracted_column_infos).strip(',') + '\n]' + '\n'
            
            # Build foreign key descriptions
            for col_name, to_table, to_col in table_fks:
                from_table = table_name
                if '`' not in str(col_name):
                    col_name = f"`{col_name}`"
                if '`' not in str(to_col):
                    to_col = f"`{to_col}`"
                    
                fk_link_str = f"{from_table}.{col_name} = {to_table}.{to_col}"
                if fk_link_str not in db_fk_infos:
                    db_fk_infos.append(fk_link_str)
        
        # Join all foreign key descriptions
        fk_desc_str = '\n'.join(db_fk_infos)
        
        return schema_desc_str.strip(), fk_desc_str.strip(), chosen_db_schem_dict
    
    def is_need_prune(self, db_id):
        """Determine if schema pruning is needed based on database complexity"""
        db_dict = self.db2dbjsons[db_id]
        avg_column_count = db_dict['avg_column_count']
        total_column_count = db_dict['total_column_count']
        
        # Prune if database is complex
        return not (avg_column_count <= 6 and total_column_count <= 30)