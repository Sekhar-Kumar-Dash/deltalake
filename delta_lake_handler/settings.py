import difflib
from pydantic import BaseModel, Extra, root_validator

class DeltaLakeHandlerConfig(BaseModel):
    """
    Configuration for DeltaLakeHandler.
    """
    delta_table_path: str
    spark_master: str = None
    spark_conf: dict = {}
    hadoop_conf: dict = {}
    
    class Config:
        extra = Extra.forbid

    @root_validator(pre=True, allow_reuse=True, skip_on_failure=True)
    def check_param_typos(cls, values):
        """Check if there are any typos in the parameters."""
        expected_params = cls.__fields__.keys()
        for key in values.keys():
            if key not in expected_params:
                close_matches = difflib.get_close_matches(key, expected_params, cutoff=0.4)
                if close_matches:
                    raise ValueError(f"Unexpected parameter '{key}'. Did you mean '{close_matches[0]}'?")
                else:
                    raise ValueError(f"Unexpected parameter '{key}'.")
        return values

    @root_validator(allow_reuse=True, skip_on_failure=True)
    def check_config(cls, values):
        """Check if config is valid."""
        spark_master = values.get("spark_master")
        delta_table_path = values.get("delta_table_path")
        
        if not delta_table_path:
            raise ValueError("delta_table_path must be provided.")

        if spark_master is None:
            raise ValueError("spark_master must be provided if running outside local mode.")
        
        return values