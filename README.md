# PyModSim

[![PyPI pyversions](https://img.shields.io/badge/python-%203.6%20%7C%203.7%20%7C%203.8%20%7C%203.9-blue)](https://pypi.python.org/pypi/python-igraph)

PyMoDSim is an easy to use agent-based simulator for mobility-on-demand (MOD) and general dynamic vehicle routing problems (DVRP). PyMoDSim reduces the time to test MOD optimization approaches, as the user does not have to spend time building the city network and the simulation framework from scratch. 

## Main Features
The main features of PyMoDSim include:
- Object-oriented design for easy usage and extension of MOD and DVRP scenarios
- Simulation of vehicles via a abstract router classes. PyMoDSim provides implementations of router class for various paths types  
    - Straight lines using [haversine formula] or [euclidean distance]
    - City network using [OpenStreetMap] via [OSMnx] graph or [OSRM]
- Ability to simulate multiple MOD scenarios including:
    - Automated vehicles (AVs) based ride sourcing
    - AVs based ride sharing
    - Electric or Gasoline vehicles based MOD
    - Inclusion of electric or gas stations
    - Combined MOD and freight services
    - MOD with repositioning of idle vehicles
    - Easy adaptability to other scenarios, like inclusion of public transports
- Realtime or offline plotting of the simulation on 2d map using [geopandas] library
    - Easily extendable plots by subclassing of realtime plots classes
- Easy implementation and benchmarking for fleet control algorithms
- Works with jupyter notebook
- Can be used for any variant of DVRP


[OpenStreetMap]: <https://www.openstreetmap.org/>
[OSMnx]: <https://github.com/gboeing/osmnx>
[OSRM]: <http://project-osrm.org/>
[haversine formula]: <https://en.wikipedia.org/wiki/Haversine_formula>
[euclidean distance]: <https://en.wikipedia.org/wiki/Euclidean_distance>
[geopandas]: <https://geopandas.org/>


## Installation

Install OSMnx in a clean virtual environment:

```
pip install pymodsim
```


## Contributing

Contributions to the project are welcome!

##License

GNU Lesser General Public License v3.0 - only
