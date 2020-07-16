API Info
========

1. Resource Base Quota
---------
[Documentation](https://developers.google.com/analytics/devguides/reporting/core/v4/resource-based-quota#query_resource_cost_response_field)

#### What & Why:
The resource based quota system is a new quota system which will enable Analytics 360 accounts to have higher sampling thresholds while using the Analytics Reporting API. This system is optional and independent of the existing limits placed on views and projects, such as those reported in the quotas page of the Google API Console.

* Sampling thresholds based on the number of sessions for the requested date range (property level):

	* Analytics Standard: 500k sessions

	* Analytics 360: 1M sessions

	* Analytics 360 using __resource based quota__: 100M sessions

#### Quota allocation
* 100,000 query cost units per day per property.
* 25,000 query cost units per hour per property.
* If you exceed your quota limit you will receive a RESOURCE_EXHAUSTED error.

#### Cost calculation

* The cost of a request is proportional to:

	* The size of the date range.

	* The number of hits within the view.

	* The cardinality of the requested dimensions.

	* The number of requested dimensions and metrics.

	* The complexity of segment and filter definitions.

	* The processing status of the requested information (requesting today's data is more costly than requesting yesterday's data).

	* The presence of Query Time import dimensions and metrics.
	
#### Notes
* The queryCost response field represents the computational cost of a request. It is not a monetary cost, and the API is free to use.
* Intraday and certain historical data (1+ years old) reports may still be subject to sampling, regardless of the useResourceQuotas setting.
* If a higher sampling threshold could not be satisfied for a request, the resource quota tokens will not be deducted (sampled responses always have zero resource quota cost).
* A report contains sampled data if fields samplesReadCounts, samplingSpaceSizes are present in the response.