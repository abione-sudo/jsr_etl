import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options=PipelineOptions(
    runner='DirectRunner',
    job_name='practice-beam-job'
)

with beam.Pipeline(options=options) as p:

    # Example transformation: Read from a text file, convert to uppercase, and write to another text file
   
    (
        p
        | "Create" >> beam.Create(["apple", "banana", "cherry"])
        | "Uppercase" >> beam.Map(str.upper)
        | "Print" >> beam.Map(print)
    )