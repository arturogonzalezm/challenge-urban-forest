# Urban Forest Challenge

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
- PySpark 2.3.2

## Instructions

Click the PyCharm's play button to run pyspark_get_greenest_suburbs.py

## Results/Output

```commandline
+--------------------+-----------+-----+
|         suburb_name|suburb_code|  per|
+--------------------+-----------+-----+
|           Parkville|  206041124|0.216|
|  South Yarra - West|  206041125| 0.21|
|           Southbank|  206041126|0.205|
|      East Melbourne|  206041119|0.188|
|             Carlton|  206041117|0.156|
|   Kensington (Vic.)|  206041121|0.134|
|     North Melbourne|  206041123|0.131|
|           Melbourne|  206041122|0.088|
|Flemington Raceco...|  206041120| 0.07|
|Carlton North - P...|  206071140|0.069|
|Port Melbourne In...|  206051131|0.028|
|           Docklands|  206041118|0.026|
|      West Melbourne|  206041127|0.021|
|   Prahran - Windsor|  206061136|  0.0|
|     Richmond (Vic.)|  206071144|  0.0|
|         Collingwood|  206071141|  0.0|
|           Brunswick|  206011105|  0.0|
|          Ascot Vale|  206031113|  0.0|
|      Port Melbourne|  206051130|  0.0|
|          Flemington|  206031115|  0.0|
+--------------------+-----------+-----+
```
## PySpark UI Benchmarks

![benchmarks](https://github.com/arturosolutions/challenge-urban-forest/blob/master/images/benchmarks.png)

## Hardware Specs

![specs](https://github.com/arturosolutions/challenge-urban-forest/blob/master/images/specs.png)

----

MIT License

Copyright (c) 2019 Arturo Gonzalez

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
