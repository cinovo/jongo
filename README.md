<img src="https://github.com/bguerout/jongo/raw/gh-pages/assets/img/jongo_big.png" alt="Jongo logo" title="Jongo" align="right">

### Jongo, Query in Java as in Mongo shell

**Faithful spirit**, Mongo query language isn't available in Java, Jongo fixes that. Copy/paste your queries to string.

**Object oriented**, Save & find objects into & from collections. Use embedded Jackson marshalling or your own.

**Wood solid**, As fast as Mongo Java driver. Open source, fully tested & made of rock solid libraries.

Documentation available at <a href="http://www.jongo.org/">jongo.org</a>

Java docs at https://jongo.ci.cloudbees.com/job/jongo-ci/site/apidocs/index.html


### Adds Document versioning

This fork was made to add versioning capabilities to the Jongo framework

* Initialize Jongo with the additional parameter: ``audited=true``
* Implement the saved pojos with the interface ``IWithVersion``

This will create a second collection for each collection with the suffix _history, storing all versions of the documents.

#### Get historical documents

Use following query to get all versions of a distinct document:


	db.MyCollection_history.find({_docId: "myDocumentId"}).sort({"_version": -1})

	
