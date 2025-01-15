This project was originally done for ITCS-5122 ( Visual Analytics ) at the University of North Carolina at Charlotte. The requirement was to create a Tableau Dashboard on a topic that interests you from any data source. Typically this would mean that you just take data from a pre-collected dataset from Kaggle, but where's the fun in that?

The first thing this project does is collect game information from steamspy api and active player counts from steam api. The basic idea is that every 10 minutes, DataFetch.py will query for the top 100 games of the past 2 weeks and add them to a games table. Then we will query for active player count of games in games table by hittin steam's api and add it to a player count table. 

This project uses MySQL as a the database management system. You can configure your connection to MySQL in config.yaml. You can also change what functions run when in config.yaml.

The second thing this project does is create a dashboard. When this project was orignally made, I had access to a student Tableau account where I made the [dashboard](https://public.tableau.com/app/profile/sullivan.crouse/viz/IndividualProject_17086389365520/SteamPlayerCountAnalysis) for this project. However, I no longer have access to Tableau so Dashboard.py is my attempt to recreate the dashboard in python.

Future work in this project would be to containerize this code such that anyone would be able to run this project so long as they have docker installed. Eventually it'd also be cool make build my own machine that I can have this code run on that is open to hit, but I need to do some serious research on security for that.
