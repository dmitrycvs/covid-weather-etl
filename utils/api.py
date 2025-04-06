def indentify_api_type(file_path):
    if "weather" in file_path.lower():
        return "weather"
    elif "covid" in file_path.lower():
        return "covid"
    
    return None