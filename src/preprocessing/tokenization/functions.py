def clean_messages(row, file_list):
  try:
    message = row['COMMIT_MESSAGE']

    # Nos quedamos hasta el primer \n
    message = message.split('\n')[0]

    # Pasamos a lowercase todo
    message = message.lower()

    # Si en la primera palabra aparece un guión la quitamos
    words = message.split()
    if '-' in words[0]:
      words = words[1:]

    # Remove first word if it is a character
    first_word_strange = not words[0][0].isalpha() and not words[0][0].isnumeric()
    while first_word_strange:
      words = words[1:]
      first_word_strange = not words[0][0].isalpha() and not words[0][0].isnumeric()
    
    # Substitute filename per token [FILENAME]
    for i in range(len(words)):
      for file in file_list:
        if words[i] == file.lower() or words[i] == file.split('.')[0].lower():
          words[i] = '[FILENAME]'
      if i > 1 and words[i - 2] == 'submitted' and words[i - 1] == 'by':
        words[i] = '[USERNAME]'
      
    return ' '.join(words)
  except:
    return ''
