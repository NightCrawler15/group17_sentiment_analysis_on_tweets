from mrjob.job import MRJob
import re
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
  
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
        txt = txt.lower()
        return txt

    def mapper(self, _, line):
        # seperating comma seperated
        line = line.strip() # removing unwanted white space
        # Doing some initial Fltering
        column = line if line != '' else "This is a dummy text!"
        # Doing some initial Fltering
        txt = self.filterText(str(column))
        # Tokenizing the words and converting into UTF-8
        # Sentiment intensity analyser uses Naiive Bayes to analyse intensity of the text
        #yield line, 1
        score = SentimentIntensityAnalyzer().polarity_scores(txt)  
        if score['neg'] > score['pos']:
            yield "Negative", 1
        elif score['pos'] > score['neg']:
            yield "Positive", 1
        else:
            yield "Neutral", 1   

    def combiner(self, key, text):
        yield key, sum(text)

    def reducer(self, key, text):
        yield key, sum(text)


if __name__ == '__main__':
    MRCleanText.run()

