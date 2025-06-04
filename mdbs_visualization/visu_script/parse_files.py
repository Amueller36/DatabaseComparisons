import os 
import json
from datetime import datetime

class DatabseCaseRun: 
    def __init__(self,
                 database:str,
                 timestamp:datetime,
                 usecase:int,
                 usecase_full_name:str,
                 execution_time:float,
                 avg_cpu_percent:float,
                 peak_cpu_percent:float,
                 avg_mem_mb:float,
                 peak_mem_mb:float, 
                 ): 
        self.database = database
        self.timestamp = timestamp
        self.usecase = usecase
        self.execution_time = execution_time
        self.avg_cpu_percent = avg_cpu_percent
        self.peak_cpu_percent = peak_cpu_percent
        self.avg_mem_mb = avg_mem_mb
        self.peak_mem_mb = peak_mem_mb
        self.usecase_full_name = usecase_full_name
    @classmethod
    def from_json(cls,json:dict,run:int): 
        """Creates an instance from Json file"""    
        run_time = datetime.strptime(json['run_id'], "%Y-%m-%dT%H:%M:%S.%f")
        use_case_str = json['usecase']
        usecase_number = int(use_case_str.split("_")[0][-1:])
        use_case_clean_name = use_case_str.split("_")[1]
        return cls(
                database = json['database'],
                timestamp = run_time,
                usecase_full_name = use_case_clean_name,
                usecase = usecase_number,
                execution_time = json['execution_time_s'], 
                avg_cpu_percent= json['avg_cpu_percent'],
                peak_cpu_percent = json['peak_cpu_percent'],
                avg_mem_mb = json['avg_mem_mb'],
                peak_mem_mb = json['peak_mem_mb'],
                  )




def parse_filename(file_name:str): 
    """A Function to parse the name of the file"""
    fileparts = file_name.split("_")
    return fileparts[0], datetime.strptime(fileparts[1], "%m-%d-%Y"), int(fileparts[2].replace("Run",""))
    
def get_usecase_instances(folder_path):
    run_file_map = {}
    paths = os.listdir(folder_path)
    for p in paths: 
        filename, file_extension = os.path.splitext(p)
        full_path = os.path.join(folder_path,p)
        db_type,run_date,run_number = parse_filename(filename)
        if not run_file_map.get(run_number): 
            run_file_map[run_number] = []
            run_file_map[run_number].append(full_path)
            continue
        run_file_map[run_number].append(full_path)    
    case_db_run_instances = []
    for run, values in run_file_map.items(): 
        for v in values: 
            with open(v, 'r') as file:
                for line in file:
                    json_data = json.loads(line)
                    case_db_run_instances.append(DatabseCaseRun.from_json(json_data, run))
    return case_db_run_instances                