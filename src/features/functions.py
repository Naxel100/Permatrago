def cleanMessage(message):
    try:
        message = message.split("\n")[0].lower()

        words = message.split()
        if "-" in words[0]:
            words = words[1:]

        first_word_strange = not words[0][0].isalpha() and not words[0][0].isnumeric()
        while first_word_strange:
            words = words[1:]
            first_word_strange = not words[0][0].isalpha() and not words[0][0].isnumeric()

        return " ".join(words)
    
    except:
        return ""

def tokenizeFile(message, file_list):
    try:
        words = message.split()
        for i in range(len(words)):
            for file in file_list:
                if words[i] == file.lower() or words[i] == file.split('.')[0].lower():
                    words[i] = '[FILENAME]'
            if i > 1 and words[i - 2] == 'submitted' and words[i - 1] == 'by':
                words[i] = '[USERNAME]'

        return " ".join(words)

    except:
        return ""