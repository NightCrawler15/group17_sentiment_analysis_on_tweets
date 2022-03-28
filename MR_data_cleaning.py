from mrjob.job import MRJob
import re
  
class MRCleanText(MRJob):

    def mapper(self, _, line):
        # seperating comma seperated
        line = line.strip() # removing unwanted white space
        column = line.split(',')
        print(column)
        tweet_id = column[0]
        tweet_date = column[4]
        text = column[7]
        yield tweet_id, 1   

    def combiner(self, key, text):
        # removing url from the text
        #text = re.sub(r'^https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE)
        # removing smileys/stickers and extracting only text
        #text = " ".join(re.split("[^a-zA-Z]*", text))
        yield key, sum(text)

    def reducer(self, key, text):
        yield key, sum(text)


if __name__ == '__main__':
    MRCleanText.run()

