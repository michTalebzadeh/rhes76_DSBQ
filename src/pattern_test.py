import re

"""
in Python, the re module provides a set of functions for working with regular expressions.
These functions allow you to perform various operations such as searching, matching, and substitution using regular expression patterns.
Here are some commonly used functions in the re module:

re.search(pattern, string, flags=0)

Searches for the first occurrence of the pattern in the string.
Returns a match object if a match is found, None otherwise.

re.match(pattern, string, flags=0)

Checks if the pattern matches at the beginning of the string.
Returns a match object if the pattern matches at the beginning, None otherwise.

re.fullmatch(pattern, string, flags=0)

Checks if the entire string matches the pattern.
Returns a match object if the entire string matches, None otherwise.
re.findall(pattern, string, flags=0)

Finds all occurrences of the pattern in the string.
Returns a list of matched substrings.

re.finditer(pattern, string, flags=0)
Finds all occurrences of the pattern in the string.
Returns an iterator yielding match objects.

re.sub(pattern, repl, string, count=0, flags=0)
Substitutes occurrences of the pattern with the replacement string.
Returns a new string with substitutions.

re.compile(pattern, flags=0)
Compiles a regular expression pattern into a regular expression object.
Returns a compiled regular expression object.

re.split(pattern, string, maxsplit=0, flags=0)
Splits the string at occurrences of the pattern.
Returns a list of substrings.

These functions are part of the standard re module in Python, and they provide powerful tools for working with text data using regular expressions. The flags parameter in these functions allows you to specify additional options for the pattern matching, such as case-insensitivity or multiline matching.
"""

def regex_examples():
  # Example 1: Matching a simple pattern
  pattern1 = re.compile(r'\d+')  # Match one or more digits
  text1 = '123 abc 456'
  result1 = pattern1.findall(text1)
  print(result1)  # Output: ['123', '456']

  # Example 2: Using groups to extract specific parts
  pattern2 = re.compile(r'(\d+) (\w+)')
  text2 = '123 abc'
  result2 = pattern2.match(text2)
  if result2:
    print(result2.group(1))  # Output: '123'
    print(result2.group(2))  # Output: 'abc'

  # Example 3: Finding all email addresses in a text
  pattern3 = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
  text3 = 'Contact support@example.com or info@domain.com'
  result3 = pattern3.findall(text3)
  print(result3)  # Output: ['support@example.com', 'info@domain.com']

  # Example 4: Substituting text
  pattern4 = re.compile(r'\bapples\b')
  text4 = 'I like apples and apples are red.'
  result4 = pattern4.sub('apples', text4)
  print(result4)  # Output: 'I like apples and apples are red.'

  # Example 5: Splitting a string based on a pattern
  pattern5 = re.compile(r'\s+')
  text5 = 'This is a sentence.'
  result5 = pattern5.split(text5)
  print(result5)  # Output: ['This', 'is', 'a', 'sentence.']
  
print(regex_examples)
  
regex_examples()
  
  
text = 'This is a sentence for me.'
pattern = re.compile(r'\s+')
print(pattern.split(text))
