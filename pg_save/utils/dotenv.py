import os
from loguru import logger

def read_envfile(envfile_path: str | list[str], stop_after_first_success: bool = True) -> int:
    '''\
        Read variables and set them to os.env from the given file/list of files.  
        If multiple files are given, `stop_after_first_success` indicates if more than one should be read
        (with priority of identical variables given to the first files)
    '''
    if isinstance(envfile_path, str):
        envfile_path = [envfile_path]
    files_read = 0
    for envfile in envfile_path:
        if os.path.exists(envfile):
            logger.info("reading envfile file located at {}", envfile)
            with open(envfile, "r", encoding="utf-8") as file:
                for line in file:
                    try:
                        if len(line) == 0 or line.startswith("#"):
                            continue
                        if line.startswith("export "):
                            line = line[len("export ") :].strip()
                        name, value = line.split("=", 1)
                        if " #" in value:
                            value = value[: value.index(" #")].strip()
                        if name in os.environ:
                            logger.info('skipping env variable "{}" from envfile as it is already set', name)
                        else:
                            os.environ[name] = value.strip()
                    except Exception:
                        pass
            files_read += 1
            if stop_after_first_success:
                return files_read
    return files_read