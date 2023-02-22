import os
import sys
import stat

# 内置容器库
from collections.abc import Mapping

import yaml
import pandas as pd

cur_path = os.path.dirname(os.path.abspath(__file__)) 
MODES = stat.S_IWUSR | stat.S_IRUSR

class SchemaNode:
    def __init__(self):
        self.attrs = {}
        self.label = ''

class SchemaRelation:
    def __init__(self, start, end, label):
        self.start = start
        self.end = end
        self.label = label

class CsvUnit:
    def __init__(self, config_file: str):
        if os.path.exists(config_file):
            # 获取配置信息
            with os.fdopen(os.open(config_file, flags = os.O_RDONLY, mode = MODES), 'r') as f:
                self.config = yaml.safe_load(f)
            
            self.people_schema = self.config['PEOPLE_SCHEMA']
        else:
            raise FileNotFoundError("config file not exist")
    
    def get_data_value(self, data_type, data_value):
        if data_type == 'str':
            return str(data_value)
        if data_type == 'int':
            return int(data_value)            
        return ''

    def get_schema_nodes(self, data: Mapping):
        nodes = {}
        for n_k, n_v in self.people_schema['nodes'].items():
            node = SchemaNode()
            node.label = n_v['label']
            for k, v in n_v['attrs'].items():
                data_feat = v['feature']
                data_type = v['type']
                data_value = data.get(data_feat, None)
                # 数值有效，添加数据
                if pd.notna(data_value):
                    node.attrs[data_feat] = self.get_data_value(data_type, data_value)
            nodes[node.label] = node
        return nodes
    
    
    def get_schema_relations(self, nodes: Mapping):
        rels = {}
        for r_k, r_v in self.people_schema['relations'].items():
            start_node = nodes.get(r_v['start'], None)
            end_node = nodes.get(r_v['end'], None)
            # 数值有效，添加数据
            if pd.notna(start_node) and pd.notna(end_node):
                rel = SchemaRelation(start_node, end_node, r_v['label'])
                rels[rel.label] = rel
        return rels

    def data_to_schema(self, series: pd.Series):
        data = series.to_dict()
        node_dic = self.people_schema['nodes'].copy()
        nodes = self.get_schema_nodes(data) 
        rels = self.get_schema_relations(nodes)
        pass 

    def analysis_csv(self, csv_file, chunk_size):
        csv_its = pd.read_csv(csv_file, chunksize=chunk_size)
        for i, df in enumerate(csv_its):
            # 替换null值
            df.fillna(value='')
            # 调用read_data遍历数据，axis 0:行 1:列
            df.apply(self.data_to_schema, axis=1) 


if __name__ == "__main__":
    yml_file = os.path.join(cur_path, "../data/config_people.yml")
    csv_unit = CsvUnit(yml_file)

    csv_file = os.path.join(cur_path, "../data/people.csv")
    csv_unit.analysis_csv(csv_file, 1)