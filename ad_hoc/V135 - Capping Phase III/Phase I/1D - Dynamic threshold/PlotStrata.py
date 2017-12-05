import pandas as pd
from pylab import *
from scipy import stats

d = pd.read_csv(r'G:\RTCI\Sky Projects\Vespa\Measurements and Algorithms\Capping 3.0\1D - Dynamic threshold\out.csv')
plot(log10(d['\'Duration\'']),log10(d['\'Number_Events\'']),'o')

gradient, intercept, r_value, p_value, std_err = \
    stats.linregress(log10(d[ d['\'cdf_number_events\''] < 0.8 ]['\'Duration\'']), \
                     log10(d[ d['\'cdf_number_events\''] < 0.8 ]['\'Number_Events\'']))

plot(log10(d['\'Duration\'']),gradient*log10(d['\'Duration\''])+intercept)
plot(log10([60*60]*5),xrange(5))
plot(log10([120*60]*5),xrange(5))
plot(log10([243*60]*5),xrange(5))
ylim([-0.1,max(log10(d['\'Number_Events\'']))*1.05])
xlabel('Event Duration (log_10(seconds))')
ylabel('Number of Events (log_10)')
show()

