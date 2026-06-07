<h3>Task 4 - Detection of Vessel Collisions</h3>

<p>The objective of this examination is to 
evaluate the ability to process large-scale temporal and spatial data. 
Identify two vessels that have collided (or experienced the closest possible physical proximity indicating a collision) within a specified marine area. You must visualize their respective trajectories 
10 minutes prior to and 10 minutes following the time of collision.</p>

Given geographic center for filtering data:
Latitude: 55.225000, Longitude: 14.245000.

# Project Files

## Main Components

### `main.ipynb`

Main Jupyter Notebook containing all required logic for processing and analyzing the AISDK dataset.

### `coordinate_check.py`

Python script used to generate and validate coordinates for the interactive map visualization.

### `docker-compose.yml`

Docker Compose configuration file that defines the complete application stack required to run the Jupyter environment and supporting services.

### `requirements.txt`

List of Python package dependencies used by `pip` to install all required libraries for the project.

### `task_4_ship_paths.html`

Interactive HTML visualization displaying the collision paths and movement trajectories of Ship A and Ship B.

### `ship_a_collision`

Contains a .csv file `part-00000-fe4f12b2-81b6-4591-993a-b64ee28f9041-c000.csv` which holds all the ship A movement pings that are related to the near collision event

### `ship_b_collision`

Contains a .csv file `part-00000-fe4f12b2-81b6-4591-993a-b64ee28f9041-c000.csv` which holds all the ship B movement pings that are related to the near collision event

<h3>In order to start the container</h3>

<ul>
<li>Install docker</li>
<li>In the linux-based command line terminal and in the task__4/main  working directory type ```docker-compose up -d```. This pull the task 4 image from the docker registry</li>
<li>Open the web browser and type localhost:8888</li>
<li>In the web browser Jupyter UI, open main.ipynb jupyter notebook and you will see all the code related to the AIS data ship analysis task</li>
</ul>

<h3>Methodology</h3>

Most of the ship collision analysis is done using Python PySpark library. Created a Python notebook task__4/main/main.ipynb that utilizes PySpark cluster, partitions and grid based calculations for ship collisions   

<ul>
<li>
    <h3>Data loading from .csv files</h3>
    <ul>
    <li>Data was loaded using standard pyspark read.csv() function.</li>
    <li>Used inferSchema parameter for making sure that pyspark detects appropriate column data types based on dataset values.</li>
    <li>In pyspark read.csv() function, used parameter header=True for making
    sure that pyspark treats the first rows in the dataset files as a headers for allowing data selection based on column names using pyspark function
    select()</li>
    </ul>
</li>
<li>
    <h3>Data cleaning and preprocessing</h3>
    <ul>
    <li>After csv file loading process is finished, performed data filtering. During the data filtering process, selected records that do not contain missing values by using pyspark function 
    ```python
    isNotNull()
    ```</li>
    <li>Additionally, in order not to work with big rows in the parallel computing scripts, selected only 7 required columns, such as Timestamp, MMSI, Latitude, Longitude, Name, SOG and Heading during pyspark data filtering process</li>
    <li>In order to perform time based operations, added new data time column which is based on 24 hour format. Original timestamp column is a string, new column was needed for time based operations</li>
    <li>For using memory efficiently applied pyspark data partitioning
    based on ship MMSI and timestamp. 
    ```python
    window = Window.partitionBy("MMSI").orderBy("timestamp")
    ```
    </li>
    <li>Performed bin column creation based on timestamp, longitude and latitude and grid space. This process was done for efficient database join operations
    and preventing big sets of column combinations.</li>
    </ul>
    <li>
    Non stationary vessels were selected based knot speed (SOG column). If the ship SOG is higher than 1, than ship records are selected.
    </li>
    <li>
    Additionally, using lag operations, data partitions and MMSI grouping, calculated total vessel movement distance for checking if vessel move more than 100 meters distance in a certain time window that consists of a specific start time and end time. 100 meters threshold was empirically chosen based on research papers and data analysis experiments. Selecting 
    vessels that moved more than 100 meters, remove enough noise in the data 
    </li>
    <li>In order to detect collision between two ships, performed column inner join operation based on MMSI, time_bucket, lat_bucket and lon_bucket columns. Bucket column were created  during the start of the data pre-processing part. After join operation is completed, new virtual tables appears that has all the possible ship combinations for finding the ship collision, which happened on a specific moment in the time</li>
    <li>There are ship pairs in the dataset that could be related to two ships in stationary mode near the port or coastal guard training session in the sea. For not selecting these cases by mistake during the collision detection part, selected ships, which have heading more than 20 degrees. This may indicate that by degrees two ship are moving towards each other and collision heading may be reality</li>
</li>
<li>
    <h3>Collision detection</h3>
    <ul>
        <li>In the processing step, all the required ship distances between ship A and ship B are calculated in meters. In the code, selecting a ship pair that has the lowest distance to a collision. </li>
        <li>
        When the lowest collision distance in meters was found, based on 10 minutes time window, selected all the moved stips towards the collision time and after collision time. 
        </li>
        <li>
        After the collision point was found and ship A and B paths were found, both ship paths were plotted in a interactive 2d map. The map is interactive and uses Javascript librararies. Generated using separate python script called coordinate_check.py. The script used .csv files that were generated by the main.ipynb notebook.                                  
        </li>
    </ul>
</li>
<li>
    <h3>Results</h3>
    Colliding ships are denoted as ship A and ship B. Discovered near fishing ship collision event. Ships passed each other at a very close distance:
    ```
    mmsi_a : 219019287
    timestamp_a : 2021-12-03 16:02:32
    name_a : HG 162 NORTH OCEAN
    lat_a : 55.243472
    lon_a : 15.087747
    sog_a : 5.4
    heading_a : 220
    mmsi_b : 219021428
    timestamp_b : 2021-12-03 16:02:54
    name_b : HG 165 SOUTH OCEAN
    lat_b : 55.243482
    lon_b : 15.087745
    sog_b : 1.6
    heading_b : 173
    heading_diff : 47
    distance_m : 1.1191536635713002
    ```
    <ul>
        <li>Both of the ships have different MMSI and are moving. The ships also are non-stationary based on knot speeds and the speeds are higher than 1. Additionally, there is variation in the coordinates ship coordinates that may show sign of ship movement. The interactive map shows how the ships are moving in the sea</li>
        <li>The ships were at a distance about 1.119 meters of each other in the near collision event</li>
        <li>Based on the calculated ship heading angle, 'HG 165 SOUTH OCEAN' fishing ship was heading almost due south, while fisihing ship 'HG 162 NORTH OCEAN' was heading South-West</li>
        <li>Latitude and longitude coordinates describe the very similar positions for the ships and the longitude and latitude differs marginally</li>
        <li>Each of the fishing ships were very close and based on the plotted map. Additionally, the Python script generated Javascript application based shows that the ships were passing each near an island in the Baltic sea</li>
        <li>Likely reasons for this near close collision: human error, poor visibility, mechanical failures and etc.</li>
    </ul>
</li>

</ul>


