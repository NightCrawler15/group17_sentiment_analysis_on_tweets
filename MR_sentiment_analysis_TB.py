from mrjob.job import MRJob
import re
from textblob import Blobber
from textblob.sentiments import NaiveBayesAnalyzer
# Initializing Native Bayes analyzer
# Sentiment intensity analyser uses Naiive Bayes to analyse intensity of the text
blob = Blobber(analyzer=NaiveBayesAnalyzer())

class MRCleanText(MRJob):
    def mapper(self, _, line):
        # seperating comma seperated
        line = line.strip() # removing unwanted white space
        # Doing some initial Fltering
        column = line if line != '' else "This is a dummy text!" 
        # Doing some initial Fltering
        txt = str(column)
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
        # classifying into positive/ negative using naive bayes
        classifier = blob(txt).sentiment
  
        if round(classifier[1], 2) > round(classifier[2], 2):
            yield "Positive", 1
        elif round(classifier[1], 2) < round(classifier[2], 2):
            yield "Negative", 1
        else:
            yield "Neutral", 1   

    def combiner(self, key, text):
        yield key, sum(text)

    def reducer(self, key, text):
        yield key, sum(text)


if __name__ == '__main__':
    MRCleanText.run()

