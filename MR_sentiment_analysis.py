from mrjob.job import MRJob
import re
  
class MRCleanText(MRJob):
    def filterText(self, txt):
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
        txt = text.lower()
        return txt

    def mapper(self, _, line):
        # seperating comma seperated
        line = line.strip() # removing unwanted white space
        column = line.split(',')
        tweet_id = column[0]
        # Doing some initial Fltering
        txt = self.filterText(column[6])
        # Tokenizing the words and converting into UTF-8
        words = [word.encode(encoding='UTF-8',errors='strict') for word in txt.split(' ')]

        yield tweet_id, 1   

    def combiner(self, key, text):
        yield key, sum(text)

    def reducer(self, key, text):
        yield key, sum(text)


if __name__ == '__main__':
    MRCleanText.run()

