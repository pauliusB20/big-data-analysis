
Summary of the Task 1 project files

- Task_1_presentation_slides.pdf - slides for the assignment 1 defence

- profiling folder - Task 4 plots for analyzing memory usage and overhead

  Note: To open .svg flamegraph use browser (Chrome or Firefox)

- config.py - script settings

- helper.py - methods that are used for reading data and working with AIS data

- models.py - data classes that are used for analyzing AIS ship data

- parser.py - multiprocesses parser for collecting data from big data source files

- workers.py - module that has anomaly A detection, C detection, D detection worker classes for parallelization

- Chunk_analysis.py - script for generating plot for evaluating execution time

- Chunk_analysis.jpg - chunk size plot for evaluating execution time 

- dfsi_score_calculation.py - helper script for generating console report of anomaly
  dfsi scores

- services.py - helper module for detecting anomalies. Has anomaly B for detecting loitering ships


Before running the script, install required packages with virtual env:

1. python -m venv task_1
2. source task_1/bin/activate
3. pip install --no-cache-dir -r  requirements.txt
4. python3 main.py
