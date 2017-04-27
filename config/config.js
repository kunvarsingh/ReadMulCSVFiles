// configuration settings for csv

module.exports = {
	csvSettings: {
 		"primaryKey": "npi.npi_id",
		"canditateKeys": ['first_name','middle_name','last_name'],
		"supportingKeys": ['street_address'],
		"sourcePath" : "./csvFiles/",
		"outputPath" : "./finalCsv/"
    }
}