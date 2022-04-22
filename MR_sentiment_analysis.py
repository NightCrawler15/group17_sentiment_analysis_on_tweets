from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class Tweets(MRJob):
    MRJob.SORT_VALUES = True

    WORDS_DICT = {}

    def mapper_init(self):
        self.WORDS_DICT = Tweets._words_score_dict()
    
    @staticmethod
    def _words_score_dict():
        #Loading the dictionary at the time of initiation
        words_file = open('/home/ubuntu/AFINN-en-165.txt')

        words_dict = {}

        for line in words_file:
            word, score = line.split('\t')
            words_dict[word] = int(score)

        words_file.close()
        return words_dict

    @staticmethod
    def _clean_word(word):
        #Extracting only text
        results = re.findall('^#?[a-zA-Z_]*', word)

        if len(results) > 0:
            word = results[0].lower()

        return word

    @staticmethod
    def _filter_text(txt):
        # Remove mentions
        txt = re.sub(r'@[A-Za-z0-9_]+', '', txt)
        # Remove hashtags
        txt = re.sub(r'#[A-Z0-9]+', '', txt)
        # Remove retweets:
        txt = re.sub(r'RT : ', '', txt)
        # Remove urls
        txt = re.sub(r'https?:\/\/[A-Za-z0-9\.\/]+', '', txt)
        #remove amp
        txt = re.sub(r'&amp;', '', txt)
        #rempve strange characters
        txt = re.sub(r'ðŸ™', '', txt)
        #remove new lines
        txt = re.sub(r'\n', ' ', txt)
        #converting lower text
        txt = txt.lower()
        return txt
    
    def _eval_word(self, word):
        if word in self.WORDS_DICT:
            return self.WORDS_DICT[word]
        else:
            return 0
    
    # returns true if key exist
    @staticmethod
    def _field_in_dict(dictionary, field):
        return dictionary is not None and field in dictionary and dictionary[field] is not None


    def mapper(self, _, line):
        # seperating comma seperated
        line = line.strip() # removing unwanted white space
        # Doing some initial Fltering
        column = line if line != '' else "This is a dummy text!"
        txt = Tweets._filter_text(str(column))
        pos = 0
        neg = 0
        total = 0
        for word in txt.split():
            clean_wrd = Tweets._clean_word(word)
            score = self._eval_word(clean_wrd)
            total += 1
            if score >= 0:
                pos = pos + score
            else:
                neg = neg + score
        # if empty text
        if total == 0: 
            total = 1
        if round(pos/total, 5) > round(abs(neg/total), 5):
            yield "Positive", 1
        elif  round(pos/total, 5) <  round(abs(neg/total), 5):
            yield "Negative", 1
        else:
            yield "Neutral", 1

    def combiner(self, key, value):
        yield(key, sum(value))

    def reducer(self, key, value):
        yield(key, sum(value))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer
                       )]

if __name__ == '__main__':
    Tweets.run()
