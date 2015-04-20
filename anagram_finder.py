#anagram finder on hadoop

word_list = [word.strip() for word in open('word_list.txt').readlines()]

print '{0} words in list'.format(len(word_list)) #get the number of the words in txt file

print 'first 10 words: {0}'.format(', '.join(word_list[0:20])) #get the first 20 words

from mrjob.job import MRJob #import MRJob 

class MRAnagram(MRJob):
    
    def mapper(self, _, line):
        #convert word into a list of chars
        #sort them, convert back to a string
        letters = list(line)
        letters.sort()
        
        #key is the sorted word, value is the regular word
        yield letters, line
        
    def reducer(self, _, words):
        #get the list of words containing these letters
        anagrams = [w for w in words]
        
        #only yield results if there are at least
        #two words which are anagrams of each other
        if len(anagrams) > 1:
            yield len(anagrams), anagrams

#to run the code
if __name__ == '__main__':
    MRAnagram.run()
        
        
