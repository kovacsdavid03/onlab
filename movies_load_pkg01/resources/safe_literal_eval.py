import ast

def safe_literal_eval(x):
    try:
        parsed = ast.literal_eval(x)
        if isinstance(parsed, list):
            return parsed
        else:
            return []  
    except (ValueError, SyntaxError):
        return []  