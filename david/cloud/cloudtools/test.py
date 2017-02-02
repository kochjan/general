
import __builtin__
import timeit
import numpy


def run(date):  
    results = []

    print "% Numpy"
    print "% Size, Time [ms]"

    def test(m1, m2):
        numpy.dot(m1, m2)

    for i in xrange(400, 500, 20):
        m1 = numpy.random.rand(i,i).astype(numpy.float32)
        m2 = numpy.random.rand(i,i).astype(numpy.float32)
        __builtin__.__dict__.update(locals())
        stmt = "test(m1, m2)"
        timer = timeit.Timer("test(m1,m2)")

        print i, numpy.mean(timer.repeat(50, 1)) 
    
    print 'hi', date  


run(None)
