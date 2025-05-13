# -*- coding: utf-8 -*-
"""Schema manager for text-to-SQL tasks."""

import os
import sqlite3
from typing import Dict, List, Any, Tuple

from core.utils import (
    load_json_file, is_email, is_valid_date_column
)


class SchemaManager:
    """
    Implements the database schema management functionality.
    
    This class is used to load database schema information and generate
    schema descriptions in XML format.
    """
    
    def __init__(self, 
                 data_path: str, 
                 tables_json_path: str, 
                 dataset_name: str,
                 lazy: bool = False):
        """Initialize the schema manager.
        
        Args:
            data_path: Path to the database files
            tables_json_path: Path to the tables.json file
            dataset_name: Name of the dataset (e.g., 'bird', 'spider')
            lazy: Whether to load database info lazily
        """
        self.data_path = data_path.strip('/').strip('\\')
        self.tables_json_path = tables_json_path
        self.dataset_name = dataset_name
        
        # Database information storage
        self.db2infos = {}  # Summary of database info
        self.db2dbjsons = {}  # Store all db to tables.json dict
        
        # Initialize the database JSON information
        self.init_db2jsons()
        
        # Load all database info if not lazy
        if not lazy:
            self._load_all_db_info()
    
    def init_db2jsons(self):
        """Initialize the database to JSON mapping from tables.json."""
        if not os.path.exists(self.tables_json_path):
            raise FileNotFoundError(f"tables.json not found in {self.tables_json_path}")
        
        data = load_json_file(self.tables_json_path)
        for item in data:
            db_id = item['db_id']
            
            table_names = item['table_names']
            # Count tables
            item['table_count'] = len(table_names)
            
            # Calculate column statistics
            column_count_lst = [0] * len(table_names)
            for tb_idx, col in item['column_names']:
                if tb_idx >= 0:
                    column_count_lst[tb_idx] += 1
            
            # Add column stats to the item
            item['max_column_count'] = max(column_count_lst) if len(column_count_lst) > 0 else 0
            item['total_column_count'] = sum(column_count_lst)
            item['avg_column_count'] = sum(column_count_lst) // len(table_names) if len(table_names) > 0 else 0
            
            self.db2dbjsons[db_id] = item
    
    def _get_column_attributes(self, cursor, table):
        """Get column attributes from the SQLite database."""
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
        """Get unique values for columns as strings."""
        col_to_values_str_dict = {}
        key_col_list = [json_column_names[i] for i, flag in enumerate(is_key_column_lst) if flag]

        for idx, column_name in enumerate(column_names):
            # Skip primary and foreign keys
            if column_name in key_col_list:
                continue
            
            # Skip ID, email, and URL columns
            lower_column_name: str = column_name.lower()
            if (lower_column_name.endswith('id') or 
                lower_column_name.endswith('email') or 
                lower_column_name.endswith('url')):
                values_str = ''
                col_to_values_str_dict[column_name] = values_str
                continue

            # Get unique values for the column
            sql = f"SELECT `{column_name}` FROM `{table}` GROUP BY `{column_name}` ORDER BY COUNT(*) DESC"
            cursor.execute(sql)
            values = cursor.fetchall()
            values = [value[0] for value in values]

            # Get value examples as a string
            values_str = ''
            try:
                values_str = self._get_value_examples_str(values, column_types[idx])
            except Exception as e:
                print(f"\nerror: get_value_examples_str failed, Exception:\n{e}\n")

            col_to_values_str_dict[column_name] = values_str

        # Build the final list of [column_name, values_str] pairs
        col_to_values_str_lst = []
        for k, column_name in enumerate(json_column_names):
            values_str = ''
            is_key = is_key_column_lst[k]

            if is_key:
                values_str = ''
            elif column_name in col_to_values_str_dict:
                values_str = col_to_values_str_dict[column_name]
            else:
                print(f"error: column_name: {column_name} not found in col_to_values_str_dict")
            
            col_to_values_str_lst.append([column_name, values_str])
        
        return col_to_values_str_lst
    
    def _get_value_examples_str(self, values: List[Any], col_type: str):
        """Generate a string representation of example values."""
        if not values:
            return ''
        
        # Skip numeric columns with many values
        if len(values) > 10 and col_type in ['INTEGER', 'REAL', 'NUMERIC', 'FLOAT', 'INT']:
            return ''
        
        # Process values
        vals = []
        has_null = False
        for v in values:
            if v is None:
                has_null = True
            else:
                tmp_v = str(v).strip()
                if tmp_v == '':
                    continue
                else:
                    vals.append(v)
        
        if not vals:
            return ''
        
        # Handle text columns
        if col_type in ['TEXT', 'VARCHAR']:
            new_values = []
            
            for v in vals:
                if not isinstance(v, str):
                    new_values.append(v)
                else:
                    if self.dataset_name == 'spider':
                        v = v.strip()
                    if v == '':  # exclude empty string
                        continue
                    elif ('https://' in v) or ('http://' in v):  # exclude URL
                        return ''
                    elif is_email(v):  # exclude email
                        return ''
                    else:
                        new_values.append(v)
            
            vals = new_values
            tmp_vals = [len(str(a)) for a in vals]
            if not tmp_vals:
                return ''
            
            # Skip very long text
            max_len = max(tmp_vals)
            if max_len > 50:
                return ''
        
        if not vals:
            return ''
        
        # Limit number of examples
        vals = vals[:6]

        # Special handling for date columns
        is_date_column = is_valid_date_column(vals)
        if is_date_column:
            vals = vals[:1]

        # Add NULL if present
        if has_null:
            vals.insert(0, None)
        
        val_str = str(vals)
        return val_str
    
    def _load_single_db_info(self, db_id: str) -> dict:
        """Load information for a single database."""
        table2coldescription = {}  # {table_name: [(column_name, full_column_name, column_description), ...]}
        table2primary_keys = {}    # {table_name: [primary_key_column_name,...]}
        table_foreign_keys = {}    # {table_name: [(from_col, to_table, to_col), ...]}
        table_unique_column_values = {}  # {table_name: [(column_name, examples_values_str)]}

        db_dict = self.db2dbjsons[db_id]

        # Gather all primary and foreign key ids
        important_key_id_lst = []
        keys = db_dict['primary_keys'] + db_dict['foreign_keys']
        for col_id in keys:
            if isinstance(col_id, list):
                important_key_id_lst.extend(col_id)
            else:
                important_key_id_lst.append(col_id)

        # Connect to the database
        # Handle different dataset directory structures
        if self.dataset_name == "bird":
            db_path = f"{self.data_path}/dev_databases/{db_id}/{db_id}.sqlite"
        elif self.dataset_name == "spider":
            db_path = f"{self.data_path}/database/{db_id}/{db_id}.sqlite"
        else:
            db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
            
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")  # avoid encoding errors
        cursor = conn.cursor()

        # Process each table
        table_names_original_lst = db_dict['table_names_original']
        for tb_idx, tb_name in enumerate(table_names_original_lst):
            # Get column information
            all_column_names_original_lst = db_dict['column_names_original']
            all_column_names_full_lst = db_dict['column_names']
            col2dec_lst = []

            # Process columns for this table
            pure_column_names_original_lst = []
            is_key_column_lst = []
            for col_idx, (root_tb_idx, orig_col_name) in enumerate(all_column_names_original_lst):
                if root_tb_idx != tb_idx:
                    continue
                
                pure_column_names_original_lst.append(orig_col_name)
                
                # Check if this is a key column
                if col_idx in important_key_id_lst:
                    is_key_column_lst.append(True)
                else:
                    is_key_column_lst.append(False)
                
                # Get full column name
                full_col_name: str = all_column_names_full_lst[col_idx][1]
                full_col_name = full_col_name.replace('_', ' ')
                cur_desc_obj = [orig_col_name, full_col_name, '']
                col2dec_lst.append(cur_desc_obj)
            
            table2coldescription[tb_name] = col2dec_lst
            table_foreign_keys[tb_name] = []
            table_unique_column_values[tb_name] = []
            table2primary_keys[tb_name] = []

            # Get column attributes and values
            all_sqlite_column_names_lst, all_sqlite_column_types_lst = self._get_column_attributes(cursor, tb_name)
            col_to_values_str_lst = self._get_unique_column_values_str(
                cursor, tb_name, all_sqlite_column_names_lst, all_sqlite_column_types_lst, 
                pure_column_names_original_lst, is_key_column_lst
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
                err_message = f"pk_idx: {pk_idx} is not int or list"
                print(err_message)
                raise Exception(err_message)
            
            for cur_pk_idx in pk_idx_lst:
                tb_idx = all_column_names_original_lst[cur_pk_idx][0]
                col_name = all_column_names_original_lst[cur_pk_idx][1]
                tb_name = table_names_original_lst[tb_idx]
                table2primary_keys[tb_name].append(col_name)
        
        cursor.close()
        conn.close()

        # Return the result
        result = {
            "desc_dict": table2coldescription,
            "value_dict": table_unique_column_values,
            "pk_dict": table2primary_keys,
            "fk_dict": table_foreign_keys
        }
        return result
    
    def _load_all_db_info(self):
        """Load information for all databases."""
        print("\nLoading all database info...")
        
        # Get database IDs based on dataset type
        if self.dataset_name == "bird":
            db_folder = os.path.join(self.data_path, "dev_databases")
            db_ids = [item for item in os.listdir(db_folder) if os.path.isdir(os.path.join(db_folder, item))]
        elif self.dataset_name == "spider":
            db_folder = os.path.join(self.data_path, "database")
            db_ids = [item for item in os.listdir(db_folder) if os.path.isdir(os.path.join(db_folder, item))]
        else:
            db_ids = [item for item in os.listdir(self.data_path) if os.path.isdir(os.path.join(self.data_path, item))]
            
        print(f"Found {len(db_ids)} databases in {self.dataset_name} dataset")
        
        for db_id in db_ids:
            db_info = self._load_single_db_info(db_id)
            self.db2infos[db_id] = db_info
    
    def _build_table_schema_xml_str(self, table_name, columns_desc, columns_val):
        """Build an XML representation of a table schema."""
        lines = []
        lines.append(f"  <table name=\"{table_name}\">")
        
        for (col_name, full_col_name, col_extra_desc), (_, col_values_str) in zip(columns_desc, columns_val):
            lines.append(f"    <column name=\"{col_name}\">")
            
            if full_col_name and full_col_name.strip():
                lines.append(f"      <description>{full_col_name.strip()}</description>")
            
            if col_values_str:
                lines.append(f"      <values>{col_values_str}</values>")
                
            if col_extra_desc and str(col_extra_desc) != 'nan':
                lines.append(f"      <extra_info>{col_extra_desc}</extra_info>")
                
            lines.append("    </column>")
            
        lines.append("  </table>")
        return "\n".join(lines)
    
    def _build_foreign_keys_xml(self, foreign_keys):
        """Build an XML representation of foreign keys."""
        if not foreign_keys:
            return ""
            
        lines = []
        lines.append("  <foreign_keys>")
        
        for fk in foreign_keys:
            from_table, from_col, to_table, to_col = fk.split(".")
            lines.append(f"    <foreign_key>")
            lines.append(f"      <from_table>{from_table}</from_table>")
            lines.append(f"      <from_column>{from_col}</from_column>")
            lines.append(f"      <to_table>{to_table}</to_table>")
            lines.append(f"      <to_column>{to_col}</to_column>")
            lines.append(f"    </foreign_key>")
            
        lines.append("  </foreign_keys>")
        return "\n".join(lines)
    
    def _is_complex_schema(self, db_id: str):
        """Determine if schema is complex enough to need pruning."""
        db_dict = self.db2dbjsons[db_id]
        avg_column_count = db_dict['avg_column_count']
        total_column_count = db_dict['total_column_count']
        
        # Simple schemas don't need pruning
        if avg_column_count <= 6 and total_column_count <= 30:
            return False
        else:
            return True
    
    def generate_schema_description(self, db_id: str, selected_schema: dict, use_gold_schema: bool = False) -> Tuple[str, List[str], Dict]:
        """Generate database description in XML format based on the selected schema."""
        if self.db2infos.get(db_id, {}) == {}:  # lazy load
            self.db2infos[db_id] = self._load_single_db_info(db_id)
            
        db_info = self.db2infos[db_id]
        desc_info = db_info['desc_dict']    # table -> columns[(column_name, full_column_name, extra_column_desc)]
        value_info = db_info['value_dict']  # table -> columns[(column_name, value_examples_str)]
        pk_info = db_info['pk_dict']        # table -> primary keys[column_name]
        fk_info = db_info['fk_dict']        # table -> foreign keys[(column_name, to_table, to_column)]
        
        # Verify all tables are consistent
        tables_1, tables_2, tables_3 = desc_info.keys(), value_info.keys(), fk_info.keys()
        assert set(tables_1) == set(tables_2)
        assert set(tables_2) == set(tables_3)

        xml_parts = ["<database_schema>"]
        db_fk_infos = []      # use list for unique check in db

        # For selector recall and compression rate calculation
        chosen_db_schem_dict = {}  # {table_name: ['col_a', 'col_b'], ..}
        
        for (table_name, columns_desc), (_, columns_val), (_, table_fk_info), (_, table_pk_info) in \
                zip(desc_info.items(), value_info.items(), fk_info.items(), pk_info.items()):
            
            # Skip tables not in the gold schema if using gold schema
            table_decision = selected_schema.get(table_name, '')
            if table_decision == '' and use_gold_schema:
                continue

            # Get column information
            all_columns = [name for name, _, _ in columns_desc]
            primary_key_columns = [name for name in table_pk_info]
            foreign_key_columns = [name for name, _, _ in table_fk_info]

            important_keys = primary_key_columns + foreign_key_columns

            new_columns_desc = []
            new_columns_val = []

            # Process based on table decision
            if table_decision == "drop_all":
                new_columns_desc = columns_desc[:6].copy()
                new_columns_val = columns_val[:6].copy()
            elif table_decision == "keep_all" or table_decision == '':
                new_columns_desc = columns_desc.copy()
                new_columns_val = columns_val.copy()
            else:
                # Process selected columns
                llm_chosen_columns = table_decision
                append_col_names = []
                
                # First add important keys and selected columns
                for idx, col in enumerate(all_columns):
                    if col in important_keys:
                        new_columns_desc.append(columns_desc[idx])
                        new_columns_val.append(columns_val[idx])
                        append_col_names.append(col)
                    elif col in llm_chosen_columns:
                        new_columns_desc.append(columns_desc[idx])
                        new_columns_val.append(columns_val[idx])
                        append_col_names.append(col)
                
                # Add more columns if needed to reach at least 6
                if len(all_columns) > 6 and len(new_columns_val) < 6:
                    for idx, col in enumerate(all_columns):
                        if len(append_col_names) >= 6:
                            break
                        if col not in append_col_names:
                            new_columns_desc.append(columns_desc[idx])
                            new_columns_val.append(columns_val[idx])
                            append_col_names.append(col)

            # Record selected columns
            chosen_db_schem_dict[table_name] = [col_name for col_name, _, _ in new_columns_desc]
            
            # Build schema XML string
            xml_parts.append(self._build_table_schema_xml_str(table_name, new_columns_desc, new_columns_val))

            # Build foreign key information
            for col_name, to_table, to_col in table_fk_info:
                from_table = table_name
                if '`' in str(col_name):
                    col_name = col_name.replace('`', '')
                if '`' in str(to_col):
                    to_col = to_col.replace('`', '')
                    
                fk_link_str = f"{from_table}.{col_name}.{to_table}.{to_col}"
                if fk_link_str not in db_fk_infos:
                    db_fk_infos.append(fk_link_str)
        
        # Add foreign keys XML
        xml_parts.append(self._build_foreign_keys_xml(db_fk_infos))
        
        # Close the XML
        xml_parts.append("</database_schema>")
        
        # Join XML parts 
        schema_xml = "\n".join(xml_parts)
        
        return schema_xml, db_fk_infos, chosen_db_schem_dict
    
