# -*- coding: utf-8 -*-
import os
import re
import random
import json
import time
import sqlite3
from typing import Dict, List, Optional, Any


subq_pattern = r"Sub question\s*\d+\s*:"


def is_valid_date(date_str):
    if (not isinstance(date_str, str)):
        return False
    date_str = date_str.split()[0]
    if len(date_str) != 10:
        return False
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    if re.match(pattern, date_str):
        year, month, day = map(int, date_str.split('-'))
        if year < 1 or month < 1 or month > 12 or day < 1 or day > 31:
            return False
        else:
            return True
    else:
        return False


def is_valid_date_column(col_value_lst):
    for col_value in col_value_lst:
        if not is_valid_date(col_value):
            return False
    return True


def rename_file(file_path, new_name):
    """
    给定原文件路径和新文件名，重命名文件

    @param file_path: 原文件路径, 如: /home/user/test.txt
    @param new_name: 新文件名, 如: backup
    @return: 新文件路径
    """
    # 获取文件的目录和后缀名
    dir_name = os.path.dirname(file_path)
    file_name, file_ext = os.path.splitext(os.path.basename(file_path))
    
    # 获取当前时间戳
    timestamp = str(int(time.time()))
    
    # 构建新的文件名
    new_file_name = new_name + '_' + timestamp + file_ext
    
    # 构建新的文件路径
    new_file_path = os.path.join(dir_name, new_file_name)
    
    # 重命名文件
    os.rename(file_path, new_file_path)
    
    return new_file_path


def is_email(string):
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    match = re.match(pattern, string)
    if match:
        return True
    else:
        return False



def extract_world_info(message_dict: dict):
    info_dict = {}
    info_dict['idx'] = message_dict['idx']
    info_dict['db_id'] = message_dict['db_id']
    info_dict['query'] = message_dict['query']
    info_dict['evidence'] = message_dict.get('evidence', '')
    info_dict['difficulty'] = message_dict.get('difficulty', '')
    info_dict['ground_truth'] = message_dict.get('ground_truth', '')
    info_dict['send_to'] = message_dict.get('send_to', '')
    return info_dict


def replace_multiple_spaces(text):
    # 定义正则表达式，匹配多个空字符
    pattern = r'\s+'
    # 将多个空字符替换成一个空格
    new_text = re.sub(pattern, ' ', text)
    return new_text


# SQL parsing
def extract_table_names(sql_query):
    # 使用正则表达式提取FROM子句中的表名
    # 使用正则表达式提取FROM子句中的表名
    # 假设表名位于FROM关键字后面，且没有特殊字符或空格
    sql_query = sql_query.replace('`', '')
    table_names = re.findall(r'FROM\s+([\w]+)', sql_query, re.IGNORECASE) + \
                  re.findall(r'JOIN\s+([\w]+)', sql_query, re.IGNORECASE)
    return set(table_names)


def get_used_tables(sql, db_path) -> dict:  # table_name -> chosen columns & discarded columns
    table_names = extract_table_names(sql)
    sch = {}
    conn = sqlite3.connect(db_path)
    conn.text_factory = lambda b: b.decode(errors="ignore")
    cursor = conn.cursor()
    for table_name in table_names:
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = cursor.fetchall()
        column_names = [cinfo[1] for cinfo in columns]
        sch[table_name] = {
            "chosen columns": column_names,
            "discarded columns": []
        }
    return sch


def get_all_tables(db_path) -> dict:
    conn = sqlite3.connect(db_path)
    conn.text_factory = lambda b: b.decode(errors="ignore")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type=\'table\'")
    tables = cursor.fetchall()
    table_names = [a[0] for a in tables if a[0] != 'sqlite_sequence']
    sch = {}
    for table_name in table_names:
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = cursor.fetchall()
        column_names = [cinfo[1] for cinfo in columns]
        sch[table_name] = {
            "chosen columns": column_names,
            "discarded columns": []
        }
    return sch


gold_schema = []


def get_gold_columns(idx, db_path) -> dict:
    global gold_schema
    if gold_schema == []:
        input_file = "data/bird/dev_gold_schema.json"
        with open(input_file, encoding='utf8') as f:
            gold_schema = json.load(f)
    table2cols = gold_schema[idx]["columns_map"]

    sch = {}
    conn = sqlite3.connect(db_path)
    conn.text_factory = lambda b: b.decode(errors="ignore")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type=\'table\'")
    tables = cursor.fetchall()
    table_names = [a[0] for a in tables if a[0] != 'sqlite_sequence']
    for table_name in table_names:
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = cursor.fetchall()
        all_columns = [cinfo[1] for cinfo in columns]
        gold_columns = table2cols.get(table_name, [])
        gold_columns = [str(item).replace('`', '') for item in gold_columns]
        unused_columns = list(set(all_columns).difference(set(gold_columns)))
        random.shuffle(unused_columns)
        sch[table_name] = {
            "chosen columns": gold_columns + unused_columns[:3],  # used golden cols + unused random 3 cols
            "discarded columns": []
        }
    return sch


# GPT result parsing


# def parse_json(res: str) -> dict:
#     lines = res.split('\n')
#     start_idx, end_idx = -1, -1
#     for idx in range(0, len(lines)):
#         if '```json' in lines[idx]:
#             start_idx = idx
#             break
#     if start_idx == -1: return {}
#     for idx in range(start_idx + 1, len(lines)):
#         if '```' in lines[idx]:
#             end_idx = idx
#             break
#     if end_idx == -1: return {}
#     jstr = " ".join(lines[start_idx + 1: end_idx])
#     return json.loads(jstr)


# parse json output
def parse_json(res: str) -> dict:
    # lines = res.split('\n')
    # start_idx, end_idx = -1, -1
    # for idx in range(0, len(lines)):
    #     if '```json' in lines[idx]:
    #         start_idx = idx
    #         break
    # if start_idx == -1: return {}
    # for idx in range(start_idx + 1, len(lines)):
    #     if '```' in lines[idx]:
    #         end_idx = idx
    #         break
    # if end_idx == -1: return {}
    # jstr = " ".join(lines[start_idx + 1: end_idx])
    # return json.loads(jstr)
    # todo: for debug
    return {}


# check if valid format
def check_selector_response(json_data: Dict) -> bool:
    FLAGS = ['keep_all', 'drop_all']
    for k, v in json_data.items():
        if isinstance(v, str):
            if v not in FLAGS:
                print(f"error: invalid table flag: {v}\n")
                print(f"json_data: {json_data}\n\n")
                return False
        elif isinstance(v, list):
            pass
        else:
            print(f"error: invalid flag type: {v}\n")
            print(f"json_data: {json_data}\n\n")
            return False
    return True


def get_files(root, suffix):
    """
    获取指定目录下的所有指定后缀的文件
    :param root: 指定目录 str 类型  如：'.'
    :param suffix: 指定后缀 str 类型 如：'.txt'
    :return: 文件列表 
    """
    import os
    import glob
    if not os.path.exists(root):
        raise FileNotFoundError(f'path {root} not found.')
    res = glob.glob(f'{root}/**/*{suffix}', recursive=True)
    res = [os.path.abspath(p) for p in res]
    return res


# read txt file to string list and strip empty lines
def read_txt_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        print(f"load txt file from {path}")
        return [line.strip() for line in f if line.strip()!= '']

def load_json_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        print(f"load json file from {path}")
        return json.load(f)


def load_jsonl_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = []
        for line in f:
            js_str = line.strip()
            if js_str == '':
                continue
            js = json.loads(js_str)
            data.append(js)
        print(f"load jsonl file from {path}")
        return data


def append_file(path, string_lst):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a+', encoding='utf-8') as f:
        for string in string_lst:
            if string[-1] != '\n':
                string += '\n'
            f.write(string)


def save_file(path, string_lst):
    """
    保存文件
    :param path: 文件路径 str 类型
    :param string_lst: 字符串列表, 带有换行符
    """
    with open(path, 'w', encoding='utf-8') as f:
        f.writelines(string_lst)
        print(f"save file to {path}")


def save_json_file(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"save json file to {path}")


def save_jsonl_file(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        for js in data:
            f.write(json.dumps(js, ensure_ascii=False) + '\n')
        print(f"save jsonl file to {path}")


def parse_json(text: str) -> dict:
    # 查找字符串中的 JSON 块
    start = text.find("```json")
    end = text.find("```", start + 7)

    # 如果找到了 JSON 块
    if start != -1 and end != -1:
        json_string = text[start + 7: end]

        try:
            # 解析 JSON 字符串
            json_data = json.loads(json_string)
            valid = check_selector_response(json_data)
            if valid:
                return json_data
            else:
                return {}
        except:
            print(f"error: parse json error!\n")
            print(f"json_string: {json_string}\n\n")
            pass

    return {}


def preprocess_xml_content(content: str) -> str:
    """
    Preprocess XML content to fix only the issues that cause actual parsing failures.
    Focus on XML operators, not quotes (LLM can use quotes or backslashes as needed).
    
    Args:
        content: Raw XML content from LLM
        
    Returns:
        Cleaned XML content safe for parsing
    """
    
    # First pass: Handle Unicode mathematical operators globally
    # These need to be fixed everywhere, not just in specific elements
    content = content.replace('≤', '&lt;=')
    content = content.replace('≥', '&gt;=')
    content = content.replace('≠', '!=')
    content = content.replace('≈', '~=')
    
    def fix_element_content(match):
        """Fix only XML operators that cause parsing failures"""
        opening_tag = match.group(1)
        element_content = match.group(2)
        closing_tag = match.group(3)
        
        # Only fix ASCII XML operators that break parsing: < > <= >= <> &
        # Handle operators with various contexts (spaces, parentheses, word boundaries, etc.)
        element_content = re.sub(r'(\s)<=(\s|$|\)|\(|[a-zA-Z])', r'\1&lt;=\2', element_content)
        element_content = re.sub(r'(\s)>=(\s|$|\)|\(|[a-zA-Z])', r'\1&gt;=\2', element_content)
        element_content = re.sub(r'(\s)<>(\s|$|\)|\(|[a-zA-Z])', r'\1&lt;&gt;\2', element_content)
        element_content = re.sub(r'(\s)<(\s|$|\)|\(|[a-zA-Z])', r'\1&lt;\2', element_content)
        element_content = re.sub(r'(\s)>(\s|$|\)|\(|[a-zA-Z])', r'\1&gt;\2', element_content)
        
        # Fix unescaped ampersands (but not already escaped entities)
        element_content = re.sub(r'&(?!(?:amp|lt|gt|apos|quot);)', '&amp;', element_content)
        
        return f"{opening_tag}{element_content}{closing_tag}"
    
    # Apply to all XML element content
    element_pattern = r'(<[^/>]+>)(.*?)(<\/[^>]+>)'
    content = re.sub(element_pattern, fix_element_content, content, flags=re.DOTALL)
    
    return content


def validate_xml_content(content: str) -> bool:
    """
    Validate XML content for common issues before parsing.
    Uses only XML entity escaping validation.
    
    Args:
        content: XML content to validate
        
    Returns:
        True if content is likely to parse successfully
    """
    # Check for unescaped quotes in elements (common issue)
    # Look for patterns like >text with 'quotes'< that should be escaped
    quote_pattern = r'>([^<]*\'[^<]*)<'
    if re.search(quote_pattern, content):
        return False
    
    # Check for unescaped operators
    operator_pattern = r'>([^<]*\s<=\s[^<]*)<'
    if re.search(operator_pattern, content):
        return False
    
    # Check for invalid backslash escapes
    backslash_pattern = r'>([^<]*\\[<>=]+[^<]*)<'
    if re.search(backslash_pattern, content):
        return False
    
    # Check for basic XML structure
    if not re.search(r'<\w+.*?>', content):
        return False
    
    return True


def parse_xml(text: str) -> dict:
    """
    Parse XML string containing schema selection information into a Python dictionary.
    Uses regex to extract table and column information, making it more resilient to malformed XML.

    Expected format:
    <schema_selection>
      <table name="table1" selection="keep_all" />
      <table name="table2" selection="drop_all" />
      <table name="table3">
        <column>col1</column>
        <column>col2</column>
      </table>
    </schema_selection>
    """
    import re

    result = {}

    # Find all table elements with flexible regex patterns
    table_patterns = [
        # Pattern for self-closing tables with keep_all or drop_all
        r'<table\s+name\s*=\s*["\'](.*?)["\']\s+selection\s*=\s*["\'](keep_all|drop_all)["\'].*?/>',
        # Pattern for tables with name and selection attributes (non-self-closing)
        r'<table\s+name\s*=\s*["\'](.*?)["\']\s+selection\s*=\s*["\'](keep_all|drop_all)["\'].*?>.*?</table>',
        # Pattern for tables with only name attribute
        r'<table\s+name\s*=\s*["\'](.*?)["\'].*?>(.*?)</table>'
    ]

    for pattern in table_patterns:
        for match in re.finditer(pattern, text, re.DOTALL):
            if pattern.endswith(r'/>') or pattern.endswith(r'["\'].*?>.*?</table>'):
                # For tables with keep_all or drop_all
                table_name = match.group(1)
                selection = match.group(2)
                result[table_name] = selection
            else:
                # For tables with column lists
                table_name = match.group(1)
                table_content = match.group(2)

                # Find columns within this table
                columns = []
                column_matches = re.finditer(r'<column>(.*?)</column>', table_content, re.DOTALL)
                for col_match in column_matches:
                    col_name = col_match.group(1).strip()
                    if col_name:
                        columns.append(col_name)

                if columns:
                    result[table_name] = columns

    # Verify the result contains valid data
    for k, v in result.items():
        if isinstance(v, str) and v not in ['keep_all', 'drop_all']:
            print(f"Warning: invalid table selection value: {v} for table {k}")

    return result


def parse_sql(res: str) -> str:
    """Only need SQL(startswith `SELECT`) of LLM result"""
    if 'SELECT' not in res and 'select' not in res:
        res = 'SELECT ' + res
    # match = re.search(parse_pattern, res, re.IGNORECASE | re.DOTALL)
    # if match:
    #     sql = match.group().strip()
    #     sql = sql.replace('```', '') # TODO
    #     sql = sql.replace('\n', ' ') # TODO
    #     return True, sql
    # else:
    #     return False, ""
    res = res.replace('\n', ' ')
    return res.strip()


def parse_sql_from_string(input_string):
    sql_pattern = r'```sql(.*?)```'
    all_sqls = []
    # 将所有匹配到的都打印出来
    for match in re.finditer(sql_pattern, input_string, re.DOTALL):
        all_sqls.append(match.group(1).strip())
    
    if all_sqls:
        return all_sqls[-1]
    else:
        return "error: No SQL found in the input string"


def parse_single_sql(res: str) -> str:  # if do not need decompose, just one code block is OK!
    """Return SQL in markdown block"""
    lines = res.split('\n')
    iter, start_idx, end_idx = -1, -1, -1
    for idx in range(iter + 1, len(lines)):
        if '```' in lines[idx]:
            start_idx = idx
            break
    if start_idx == -1: return ""
    for idx in range(start_idx + 1, len(lines)):
        if '```' in lines[idx]:
            end_idx = idx
            break
    if end_idx == -1: return f"error: \n{res}"

    return " ".join(lines[start_idx + 1: end_idx])


def parse_qa_pairs(res: str, end_pos=2333) -> list:
    lines = res.split('\n')
    qa_pairs = []
    # end_pos = -1
    # for idx, line in enumerate(lines):
    #     if 'final SQL' in line or 'final sql' in line:
    #         end_pos = idx
    # if end_pos == -1: return []
    end_pos = len(lines) if (end_pos == 2333) else end_pos
    for idx in range(0, end_pos):
        if re.findall(subq_pattern, lines[idx], re.IGNORECASE) != []:
            query = lines[idx]
            start_idx = -1
            for idx2 in range(idx + 1, end_pos):
                if '```' in lines[idx2]:
                    start_idx = idx2
                    break
            if start_idx == -1: return []
            for idx3 in range(start_idx + 1, end_pos):
                if '```' in lines[idx3]:
                    end_idx = idx3
                    break
            if end_idx == -1: return []
            answer = " ".join(lines[start_idx + 1: end_idx])
            qa_pairs.append((str(query), str(answer)))
            idx = end_idx
    return qa_pairs


def parse_subq(res: str) -> list:
    """Only sub questions after decomposition"""
    res = '-- ' + res
    sub_qustions = []
    sub_qustions += res.split('-- ')
    sub_qustions = [q.strip() for q in sub_qustions if len(q) > 1]
    return sub_qustions


def add_prefix(sql):
    if not sql.startswith('SELECT') and not sql.startswith('select'):
        sql = 'SELECT' + sql
    return sql


# Spider data preprocess


CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
JOIN_KEYWORDS = ('join', 'on', 'as')

WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')
UNIT_OPS = ('none', '-', '+', "*", '/')
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')
TABLE_TYPE = {
    'sql': "sql",
    'table_unit': "table_unit",
}

COND_OPS = ('and', 'or')
SQL_OPS = ('intersect', 'union', 'except')
ORDER_OPS = ('desc', 'asc')


HARDNESS = {
    "component1": ('where', 'group', 'order', 'limit', 'join', 'or', 'like'),
    "component2": ('except', 'union', 'intersect')
}


def get_nestedSQL(sql):
    nested = []
    for cond_unit in sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]:
        if type(cond_unit[3]) is dict:
            nested.append(cond_unit[3])
        if type(cond_unit[4]) is dict:
            nested.append(cond_unit[4])
    if sql['intersect'] is not None:
        nested.append(sql['intersect'])
    if sql['except'] is not None:
        nested.append(sql['except'])
    if sql['union'] is not None:
        nested.append(sql['union'])
    return nested


def has_agg(unit):
    return unit[0] != AGG_OPS.index('none')


def count_agg(units):
    return len([unit for unit in units if has_agg(unit)])


def count_component1(sql):
    count = 0
    if len(sql['where']) > 0:
        count += 1
    if len(sql['groupBy']) > 0:
        count += 1
    if len(sql['orderBy']) > 0:
        count += 1
    if sql['limit'] is not None:
        count += 1
    if len(sql['from']['table_units']) > 0:  # JOIN
        count += len(sql['from']['table_units']) - 1

    ao = sql['from']['conds'][1::2] + sql['where'][1::2] + sql['having'][1::2]
    count += len([token for token in ao if token == 'or'])
    cond_units = sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]
    count += len([cond_unit for cond_unit in cond_units if cond_unit[1] == WHERE_OPS.index('like')])

    return count


def count_component2(sql):
    nested = get_nestedSQL(sql)
    return len(nested)


def count_others(sql):
    count = 0
    # number of aggregation
    agg_count = count_agg(sql['select'][1])
    agg_count += count_agg(sql['where'][::2])
    agg_count += count_agg(sql['groupBy'])
    if len(sql['orderBy']) > 0:
        agg_count += count_agg([unit[1] for unit in sql['orderBy'][1] if unit[1]] +
                            [unit[2] for unit in sql['orderBy'][1] if unit[2]])
    agg_count += count_agg(sql['having'])
    if agg_count > 1:
        count += 1

    # number of select columns
    if len(sql['select'][1]) > 1:
        count += 1

    # number of where conditions
    if len(sql['where']) > 1:
        count += 1

    # number of group by clauses
    if len(sql['groupBy']) > 1:
        count += 1

    return count


def eval_hardness(sql):
    count_comp1_ = count_component1(sql)
    count_comp2_ = count_component2(sql)
    count_others_ = count_others(sql)

    if count_comp1_ <= 1 and count_others_ == 0 and count_comp2_ == 0:
        return "easy"
    elif (count_others_ <= 2 and count_comp1_ <= 1 and count_comp2_ == 0) or \
            (count_comp1_ <= 2 and count_others_ < 2 and count_comp2_ == 0):
        return "medium"
    elif (count_others_ > 2 and count_comp1_ <= 2 and count_comp2_ == 0) or \
            (2 < count_comp1_ <= 3 and count_others_ <= 2 and count_comp2_ == 0) or \
            (count_comp1_ <= 1 and count_others_ == 0 and count_comp2_ <= 1):
        return "hard"
    else:
        return "extra"


def extract_sql_from_text(text: str) -> str:
    """
    Extract SQL query from text.
    
    Args:
        text: Text that might contain SQL
        
    Returns:
        Extracted SQL query or empty string if no SQL found
    """
    import re
    import html
    
    try:
        # Try to extract SQL from JSON
        data = parse_json(text)
        if 'sql' in data:
            return clean_sql_content(data['sql'])
        if 'final_sql' in data:
            return clean_sql_content(data['final_sql'])
            
        # Try to extract SQL with regex patterns
        sql_patterns = [
            r'```sql\s*(.*?)\s*```',  # SQL in code blocks
            r'```\s*(SELECT.*?)\s*```',    # SELECT in generic code blocks
            r"'sql':\s*'(SELECT.*?)'",     # SQL in JSON with single quotes
            r'"sql":\s*"(SELECT.*?)"',     # SQL in JSON with double quotes
            r'SELECT.*?(?=\s*[,\}\]"|$)',  # SELECT until end of SQL context
            r'WITH.*?(?=\s*[,\}\]"|$)',    # WITH queries until end of context
        ]
        
        for pattern in sql_patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            if matches:
                # Clean up the matched SQL
                sql = matches[0].strip()
                # Remove any trailing backticks or spaces
                if sql.endswith('```'):
                    sql = sql[:sql.rfind('```')].strip()
                return clean_sql_content(sql)
        
        # If no clear SQL pattern, look for any content between backticks
        code_block_pattern = r'```(.*?)```'
        code_blocks = re.findall(code_block_pattern, text, re.DOTALL)
        for block in code_blocks:
            if 'SELECT' in block.upper() or 'WITH' in block.upper():
                return clean_sql_content(block.strip())
                
        return ""
    except Exception as e:
        print(f"Error extracting SQL: {str(e)}")
        return ""


def clean_sql_content(sql_content: str) -> str:
    """
    Clean SQL content by decoding XML entities only.
    Uses only XML entity escaping, no CDATA support.
    
    Args:
        sql_content: Raw SQL content that may contain XML entities
        
    Returns:
        Cleaned SQL content
    """
    import re
    import html
    
    if not sql_content:
        return ""
    
    # Decode XML entities only (no CDATA processing)
    sql_content = html.unescape(sql_content)
    
    # Remove common trailing artifacts from JSON extraction
    artifacts = [
        r'",\s*[\'"]error_message[\'"].*$',  # Remove error_message artifacts
        r'",\s*[\'"].*[\'"]:\s*[\'"].*$',     # Remove key-value pairs after SQL
        r'\s*}\s*$',                         # Remove trailing braces
        r'\s*]\s*$',                         # Remove trailing brackets
    ]
    
    for artifact_pattern in artifacts:
        sql_content = re.sub(artifact_pattern, '', sql_content, flags=re.DOTALL)
    
    # Clean up whitespace and normalize line breaks
    sql_content = re.sub(r'\\n', '\n', sql_content)  # Convert escaped newlines
    sql_content = re.sub(r'\s+', ' ', sql_content.strip())
    
    return sql_content


# XML Parsing Utilities for Agent Communication
def extract_xml_content(text: str, tag_name: str) -> Optional[str]:
    """
    Extract content between XML tags from text.
    
    Args:
        text: Text containing XML
        tag_name: Name of the XML tag to extract
        
    Returns:
        Content between tags or None if not found
    """
    import re
    
    # Try to find the tag in the text
    pattern = rf'<{tag_name}>.*?</{tag_name}>'
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group()
    
    # For root tag only, also try to find in code blocks with ```tag_name
    code_block_pattern = rf'```{tag_name}\s*\n?(.*?)\n?```'
    match = re.search(code_block_pattern, text, re.DOTALL)
    if match:
        xml_content = match.group(1).strip()
        # Check if the content already has the root tag, if not wrap it
        if not xml_content.startswith(f'<{tag_name}>'):
            xml_content = f'<{tag_name}>\n{xml_content}\n</{tag_name}>'
        return xml_content
    
    return None


def clean_xml_content(content: str) -> str:
    """
    Clean XML content by decoding XML entities only.
    Uses only XML entity escaping, no CDATA support.
    
    Args:
        content: Raw XML content that may contain XML entities
        
    Returns:
        Cleaned XML content suitable for XML parsing
    """
    import re
    import html
    
    if not content:
        return content
    
    # Decode XML entities
    content = html.unescape(content)
    
    # Convert common HTML code tags to backticks
    content = re.sub(r'<code>(.*?)</code>', r'`\1`', content, flags=re.DOTALL)
    content = re.sub(r'<em>(.*?)</em>', r'*\1*', content, flags=re.DOTALL)
    content = re.sub(r'<strong>(.*?)</strong>', r'**\1**', content, flags=re.DOTALL)
    
    # Handle XML entities that might cause parsing issues
    # Only escape what needs to be escaped for XML parsing
    def escape_problematic_content(text):
        # Only escape content that's not already part of valid XML tags
        # Look for content that contains < or > but isn't part of a valid XML tag
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # If line contains XML tags, be careful about escaping
            if '<' in line and '>' in line:
                # Check if it's a valid XML line or contains problematic content
                if not re.match(r'^\s*<[a-zA-Z_][\w\-]*[^>]*>.*</[a-zA-Z_][\w\-]*>\s*$', line) and \
                   not re.match(r'^\s*<[a-zA-Z_][\w\-]*[^>]*/>\s*$', line) and \
                   not re.match(r'^\s*<[a-zA-Z_][\w\-]*[^>]*>\s*$', line) and \
                   not re.match(r'^\s*</[a-zA-Z_][\w\-]*>\s*$', line):
                    # This line has < > but doesn't look like valid XML
                    line = line.replace('<', '&lt;').replace('>', '&gt;')
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    content = escape_problematic_content(content)
    
    return content


def parse_xml_hybrid(text: str, root_tag: str) -> Optional[Dict[str, Any]]:
    """
    Parse XML with hybrid approach: try full parsing first, then section-by-section.
    
    Args:
        text: Text containing XML
        root_tag: Root tag name (e.g., 'schema_linking', 'evaluation')
        
    Returns:
        Parsed dictionary or None if parsing fails
    """
    import xml.etree.ElementTree as ET
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Extract XML content
    xml_content = extract_xml_content(text, root_tag)
    if not xml_content:
        logger.error(f"No {root_tag} XML found in text")
        return None
    
    # Preprocess content to fix XML entity escaping issues  
    xml_content = preprocess_xml_content(xml_content)
    xml_content = clean_xml_content(xml_content)
    
    # Try to parse the entire XML
    try:
        root = ET.fromstring(xml_content)
        return xml_element_to_dict(root)
    except ET.ParseError as e:
        logger.warning(f"Full XML parsing failed: {str(e)}")
        
        # Show the specific line and character causing the error
        error_str = str(e)
        if 'line' in error_str and 'column' in error_str:
            import re
            match = re.search(r'line (\d+), column (\d+)', error_str)
            if match:
                line_num = int(match.group(1))
                col_num = int(match.group(2))
                lines = xml_content.split('\n')
                if line_num <= len(lines):
                    problem_line = lines[line_num - 1]  # Convert to 0-indexed
                    logger.warning(f"Problem line {line_num}: '{problem_line}'")
                    if col_num <= len(problem_line):
                        problem_char = problem_line[col_num-1]
                        logger.warning(f"Problem character at column {col_num}: '{problem_char}' (ASCII: {ord(problem_char)})")
                        # Show context around the problem
                        start = max(0, col_num-10)
                        end = min(len(problem_line), col_num+10)
                        context = problem_line[start:end]
                        logger.warning(f"Context: '{context}'")
        
        logger.info("Attempting section-by-section parsing...")
        
        # Extract sections and parse individually
        return parse_xml_sections(xml_content, root_tag)


def xml_element_to_dict(element: Any) -> Dict[str, Any]:
    """
    Convert XML Element to dictionary recursively.
    
    Args:
        element: XML Element object
        
    Returns:
        Dictionary representation of the XML element
    """
    import xml.etree.ElementTree as ET
    import html
    
    def decode_xml_entities(value):
        """Decode XML entities in string values"""
        if isinstance(value, str):
            return html.unescape(value)
        elif isinstance(value, dict):
            return {k: decode_xml_entities(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [decode_xml_entities(item) for item in value]
        return value
    
    result = {}
    
    # Add attributes
    if element.attrib:
        result.update(element.attrib)
    
    # Process children
    children = list(element)
    if children:
        child_dict = {}
        for child in children:
            child_data = xml_element_to_dict(child)
            
            # Handle multiple children with same tag
            if child.tag in child_dict:
                # Convert to list if not already
                if not isinstance(child_dict[child.tag], list):
                    child_dict[child.tag] = [child_dict[child.tag]]
                child_dict[child.tag].append(child_data)
            else:
                child_dict[child.tag] = child_data
        
        # Merge child dict with result
        for key, value in child_dict.items():
            if key in result:
                # Handle conflict between attribute and child element
                result[f"_{key}"] = value
            else:
                result[key] = value
    
    # Add text content if exists
    if element.text and element.text.strip():
        text = element.text.strip()
        if children or element.attrib:
            result['_text'] = text
        else:
            return decode_xml_entities(text)
    
    # Decode XML entities in the final result
    result = decode_xml_entities(result)
    
    return result if (children or element.attrib or not element.text) else ""


def parse_xml_sections(xml_content: str, root_tag: str) -> Dict[str, Any]:
    """
    Parse XML by extracting and parsing individual sections.
    
    Args:
        xml_content: XML content as string
        root_tag: Root tag name
        
    Returns:
        Dictionary with parsed sections
    """
    import re
    import xml.etree.ElementTree as ET
    import logging
    
    logger = logging.getLogger(__name__)
    result = {}
    
    # Extract content between root tags
    root_pattern = rf'<{root_tag}[^>]*>(.*?)</{root_tag}>'
    root_match = re.search(root_pattern, xml_content, re.DOTALL)
    
    if not root_match:
        logger.error(f"Could not extract {root_tag} content")
        return result
    
    inner_content = root_match.group(1)
    
    # Find all top-level sections
    # Match both self-closing and regular tags
    section_pattern = r'<(\w+)(?:\s+[^>]*)?>.*?(?:</\1>|/>)'
    sections = re.finditer(section_pattern, inner_content, re.DOTALL)
    
    for section_match in sections:
        section_xml = section_match.group()
        tag_name = section_match.group(1)
        
        try:
            # Clean the section XML before parsing
            cleaned_section_xml = clean_xml_content(section_xml)
            # Try to parse this section
            section_element = ET.fromstring(cleaned_section_xml)
            result[tag_name] = xml_element_to_dict(section_element)
        except ET.ParseError:
            logger.warning(f"Failed to parse section {tag_name}, using regex extraction")
            # Fallback to regex extraction for this section
            result[tag_name] = extract_section_with_regex(section_xml, tag_name)
    
    return result


def extract_section_with_regex(section_xml: str, tag_name: str) -> Any:
    """
    Extract section content using regex when XML parsing fails.
    
    Args:
        section_xml: XML section as string
        tag_name: Tag name of the section
        
    Returns:
        Extracted content
    """
    import re
    
    # Handle self-closing tags
    if section_xml.rstrip().endswith('/>'):
        # Extract attributes
        attrs = {}
        attr_pattern = r'(\w+)\s*=\s*["\']([^"\']*)["\']'
        for match in re.finditer(attr_pattern, section_xml):
            attrs[match.group(1)] = match.group(2)
        return attrs
    
    # Extract inner content
    content_pattern = rf'<{tag_name}[^>]*>(.*?)</{tag_name}>'
    content_match = re.search(content_pattern, section_xml, re.DOTALL)
    
    if content_match:
        inner = content_match.group(1).strip()
        
        # Special handling for SQL-related sections
        if tag_name in ['sql', 'final_sql', 'selection']:
            # For SQL sections, look for common SQL-related patterns
            sql_patterns = [
                r'<sql>\s*(.*?)\s*</sql>',  # Plain SQL
                r'<final_sql>\s*(.*?)\s*</final_sql>',  # Plain final_sql
            ]
            
            for pattern in sql_patterns:
                sql_match = re.search(pattern, inner, re.DOTALL | re.IGNORECASE)
                if sql_match:
                    sql_content = sql_match.group(1).strip()
                    return clean_sql_content(sql_content)
        
        # Check if it contains sub-elements
        if '<' in inner and '>' in inner:
            # Has sub-elements, extract them
            sub_dict = {}
            element_pattern = r'<(\w+)[^>]*>(.*?)</\1>'
            for match in re.finditer(element_pattern, inner, re.DOTALL):
                sub_tag = match.group(1)
                sub_content = match.group(2).strip()
                
                # Clean SQL content for SQL-related tags
                if sub_tag in ['sql', 'final_sql']:
                    sub_content = clean_sql_content(sub_content)
                
                if sub_tag in sub_dict:
                    if not isinstance(sub_dict[sub_tag], list):
                        sub_dict[sub_tag] = [sub_dict[sub_tag]]
                    sub_dict[sub_tag].append(sub_content)
                else:
                    sub_dict[sub_tag] = sub_content
            
            return sub_dict if sub_dict else inner
        else:
            # Plain text content
            return inner
    
    return None


# Helper functions for common patterns
def strip_quotes(value: str) -> str:
    """Strip surrounding quotes from a value if present."""
    if value and len(value) >= 2:
        if (value.startswith("'") and value.endswith("'")) or \
           (value.startswith('"') and value.endswith('"')):
            return value[1:-1]
    return value


def ensure_list(value: Any) -> List[Any]:
    """Ensure value is a list, converting single item to list if needed."""
    if value is None:
        return []
    if not isinstance(value, list):
        return [value]
    return value
