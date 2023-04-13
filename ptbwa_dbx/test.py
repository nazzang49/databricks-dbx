# import re
#
# def camel_to_snake(camel_string):
#     # Use regular expressions to split the string by capital letters
#     snake_string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_string)
#     snake_string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake_string)
#     # Convert to lower case and return the snake case string
#     return snake_string.lower()