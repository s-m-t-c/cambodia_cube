# cambodiacubenotebooks
A collection of jupyter notebooks for drought assessment using the open data cube framework

SPEI_cambodia.ipynb : Calculates SPEI and quartile date ranges

call_climate_indices.sh : Shell script called by SPEI_cambodia.ipynb that boots a virtual environment with the requisite modules as specified in: https://climate-indices.readthedocs.io/en/latest/

SPEI_geomedians_cambodia.ipynb : Exploratory notebook for creating geomedians based on the SPEI quartiles in smaller study sites

data_visualisation.ipynb : A notebook for creating plots and animations from the SPEI and geomedian data including timeseries and heatmaps.

spei_stats.py : Creates geomedians from non-sequential dates in a tiled format using the ODC framework

stats_config.yaml : Specifies the products and metadata to calculate the geomedians in spei_stats.py

overview.pbs : Generates multi-band geotiff from the tiled output of spei_stats.py and masks it with a shapefile
