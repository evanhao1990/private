* Instructions for CM1 query *

* Set variables in the top - In case there's no need for a specific filter delete the WHERE clause (inside the set)
   ** When filtering per marketplace there is one line commented in the code that has to be included (around 154)

* Choose attributes for which you want to segment the estimated values - All types of attributes shall be possible to query withtout
       modifying any code

* Run Query and get the estimated CM1 value


* Extras *
* The folder data has copies, in csv files, of the costs which are queried in the CM1 calculation. Changes in costs shall be updated 
        in the csv's and then imported into the respective 'dim' tables in the DWH (sandbox)

* Confluence page documentation: https://bestseller.jira.com/wiki/spaces/PORTAL/pages/223664725/CM1+Logic?focusedCommentId=235734414#comment-235734414