# Pipelines

## Definition

A *pipeline* in the context of data processing or data science is a set of
defined sequential steps applied to a data input. A simple example might
be preparing text for import into a system: the pipeline might consist of
first removing duplicated spaces, then a second step to remove punctuation,
a third to convert character encodings, and a fourth to normalize to lower
case. On the more complex end, a real-time sentiment analysis system might
begin with a step that scans the Twitter API for new tweets, then a step
to determine ones which are relevant to an ongoing televised debate, a
third step to do additional filtering to eliminate tweets from bot
accounts, a fourth step to convert the tweet contents into some vector
embedding format, and a fifth to update a model of audience response to
the ideas being discussed.

In all cases, a pipeline consists of several steps that can run in sequence
without manual intervention, where later steps depend on the results of
earlier ones.

## hither Support

Pipelines within hither can exist within a single hither function, or
may be composed of multiple hither functions which interact.

Obviously the easiest way to incorporate your pipeline into hither
is to have one hither function that executes all the steps of your
pipeline. Since hither does not alter the internals of hither functions,
your pipeline will now run in whatever containerized environment you
wish.

### Pipeline-aware hither

However, hither can do much more with your pipelines if you let it.
Recall that a [hither Job is a hither function, along with its
inputs and intended execution
context](./faq.md#What-is-the-difference-between-a-hither-function-and-a-hither-job).
Whenever one Job is used as input for another, hither recognizes the attempt
to create a pipeline of operations, and makes sure to schedule the
earlier steps of the pipeline before the later ones.

hither also offers two features that help make pipelines more efficient.
First, when a Job depends on multiple independent prior Jobs in the
pipeline, those prior Jobs will be run in parallel. And second, when
a job cache is used, any Job which has already been run can be replaced
by its return value. Thus, if one step in a pipeline feeds into many
different subsequent steps (for instance, if the results of a data-cleaning
step are consumed by several different subsequent operations) hither will
ensure that the earlier step is executed only once.

[Our FAQ has an example of code to set up a hither pipeline.](./faq.md#what-is-a-simple-example-of-a-hither-pipeline)

### Pipelines across environments

Where hither really shines is in its ability to let you compose pipelines
out of code drawn from different sources, in keeping with its design goal
of facilitating code reuse. Because a hither pipeline is composed of many
Jobs, and because each Job runs in its own container environment, methods
from different labs can be easily combined into a single pipeline.

For example, suppose you wanted to determine whether a particular method's
improved performance was due to improved modeling algorithms or improved
data cleaning. You could set up the data cleaning operation from the published
code as a hither function, and then feed the results of this operation into
many different modeling algorithms, to do an objective comparison of the
results.

Another useful option is using hither Jobs to create a pipeline out of
tools with different dependencies. For instance, you could write your
own data cleaning and manipulation functions in Python, and feed them to
a statistical analysis tool implemented in Octave, and ship the output of
this analysis to a suite of Javascript tools which will create visualizations.
Since every step can operate in its own environment, your overall
pipeline can integrate all these different tools with minimal changes.

TODO: __EXAMPLE OF MULTIPLE-LANGUAGE HITHER USE__
