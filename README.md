# Eliiza Urban Forest Challenge

## Intro

In this challenge you're given data about some of Melbourne's **Statistical Areas** (as defined by the ASGS - Australian 
Statistical Geography Standard) and about the City of Melbourne's **urban forest**.  Statistical Areas are given in the form 
of polygons representing their borders.  The urban forest is given in the form of polygons too, where they represent 
the area occupied by tree canopies or bush.  In both cases, polygon vertices are longitude/latitude pairs.

More information about Statistical Areas can be found at the ASGS web site:
http://www.abs.gov.au/websitedbs/D3310114.nsf/home/Australian+Statistical+Geography+Standard+(ASGS).

## Challenge

The challenge consists in determining the greenest suburb of Melbourne, where greenest is the suburb with the highest 
vegetation per area ratio.

Some relevant info here:
- Suburbs are related to Statistical Areas Level 2 (SA2s).
- File **melb_inner_2016.json** contains the Statistical Areas of inner Melbourne.
- Directory **melb_urban_forest_2016.txt** contains the urban forest of the City of Melbourne council.
- In **both** datasets coordinates are **longitude/latitude** pairs.
- You can check http://s2map.com to play around with plotting polygons on a real globe map.

## Specifications

- PyCharm
- Python 3.6.5
- PySpark 2.3.1

## Instructions

Run read_json.py


