from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w.'\s]+")


class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words)
        ]

    def mapper_get_words(self, _, line):
        # yield each word in the line
        data = WORD_RE.findall(line)
        crime = data[0]
        yield (crime, 1)

    def reducer_count_words(self, word, counts):
        size, total = 0, 0
        for val in counts:
            size += 1
        yield ('Type of Crime:' + ' ' + word,size)


if __name__ == '__main__':
    MRMostUsedWord.run()

