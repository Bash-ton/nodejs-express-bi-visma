const { BigQuery } = require("@google-cloud/bigquery")
const util = require("util")
// const { clearBigQueryData } = require("./bigQueryFunctions")
// const { syncData, getBearerToken } = require("./vismaFunctions")

const ping = async (req, res) => {
  const senderMessage = `${
    req.query.name || req.body.name || "Function Initiated"
  }!`
  console.info(senderMessage)
  //res.status(200).send(senderMessage);

  try {
    // Visma vars TODO
    // const vismaClientId = process.env.visma_client_id;
    // const vismaClientSecret = process.env.visma_client_secret;
    // const vismaAPIScope = process.env.visma_api_scope;
    // const vismaAPIUrl = 'https://api.severa.visma.com/rest-api/v1.0';
    const vismaClientId = "UmainAB_TzQOfpgtX3AxL8DhtE.apps.vismasevera.com" //process.env.visma_client_id;
    const vismaClientSecret = "b35ca7fa-1299-a40f-0164-8be6eb99640b" //process.env.visma_client_secret;
    const vismaAPIScope =
      "projects:read customers:read users:read invoices:read hours:read" //process.env.visma_api_scope;
    const vismaAPIUrl = "https://api.severa.visma.com/rest-api/v1.0"
    if (!vismaClientId || !vismaClientSecret || !vismaAPIScope) {
      throw new Error("Missing required environment variables - Visma")
    }

    // BigQuery vars TODO
    const projectId = "prd-euw-vismaexporter-apegroup"
    const bigquery = new BigQuery({
      projectId: projectId,
    })
    const datasetId = "Umain_Visma_Dev"
    const tableId = "ARC_Umain_Time"
    const fullTableID = projectId + "." + datasetId + "." + tableId
    if (!projectId || !datasetId || !tableId) {
      throw new Error("Missing required environment variables - BigQuery")
    }
    const absenceProjectId = "9999-998"

    // Remove old BigQuery rows for current year as to not create duplicated data
    let runCleanUp = true
    if (runCleanUp) {
      console.info("INITIATE DELETING OLD BIGQUERY DATA...")
      const success = await clearBigQueryData(
        bigquery,
        fullTableID,
        absenceProjectId
      )
      if (!success) {
        return
      }
    } else {
      console.info("Skipped cleanup")
    }

    // generate authtoken
    const bearerToken = await getBearerToken(
      vismaClientId,
      vismaClientSecret,
      vismaAPIScope,
      vismaAPIUrl
    )

    //fetch data from Visma, format data and write to BigQuery
    await syncData(
      bearerToken,
      vismaClientId,
      vismaAPIUrl,
      bigquery,
      datasetId,
      tableId
    )

    console.info(`Finished sync`)
    res.apiResponse(senderMessage)
    // res.status(200).send(senderMessage)
  } catch (error) {
    // res.status(500).send()
    res.apiResponse("FAILED")
    console.error("Error:", error.message)
  }

  // res.apiResponse('pong')
}

/**
 * VISMA
 */



// Generates Visma auth token
const getBearerToken = async (
  vismaClientId,
  vismaClientSecret,
  vismaAPIScope,
  vismaAPIUrl
) => {
  try {
    const response = await axios.post(`${vismaAPIUrl}/token`, {
      client_Id: vismaClientId,
      client_Secret: vismaClientSecret,
      scope: vismaAPIScope,
    })
    return response.data.access_token
  } catch (error) {
    console.error("Error:", error.message)
  }
}

// Fetch all hours
const syncData = async (
  bearerToken,
  vismaClientId,
  vismaAPIUrl,
  bigquery,
  datasetId,
  tableId
) => {
  try {
    let totalEntries = 0 // Total entries written to Bi
    let pageToken = "" // PageToken used for looping through paginated Visma data from the API.
    let currentPage = 0 // Keep track of current paginated list of Visma data, where each page contains maximum 100 records.
    const currentYear = new Date().getFullYear()

    console.info("INITIATE FETCHING ALL ENTRIES FOR YEAR: ", currentYear)

    do {
      const response = await axios.get(`${vismaAPIUrl}/workhours`, {
        params: {
          eventDateStart: `${currentYear}-01-01`,
          eventDateEnd: `${currentYear}-12-31`,
          pageToken: pageToken,
        },
        headers: {
          Authorization: `Bearer ${bearerToken}`,
          client_Id: vismaClientId,
        },
      })

      // Format data and fetch custom fields
      let formattedData = await formatHoursData(
        response.data,
        vismaAPIUrl,
        bearerToken,
        vismaClientId
      )

      console.info(`WRITING BATCH #${currentPage} TO BIG QUERY...`)
      await writeToBigQuery(formattedData, bigquery, datasetId, tableId)
      console.info(`DONE - BATCH #${currentPage}`)

      currentPage++
      totalEntries = totalEntries + formattedData.length

      // Check if there is a "NextPageToken" in the headers, if so we have more data to fetch from Visma.
      const nextPageToken = response.headers["nextpagetoken"]

      if (nextPageToken) {
        pageToken = nextPageToken
      } else {
        // No more pages, we have fetched all data
        break
      }
    } while (true)

    console.info("Total entries added to BigQuery:", totalEntries)
  } catch (error) {
    console.error("Error:", error.message)
  }
}

const formatHoursData = async (
  rawData,
  vismaAPIUrl,
  bearerToken,
  vismaClientId
) => {
  try {
    if (!rawData || rawData.length === 0) {
      return []
    }

    const processedData = []
    const batchSize = 5
    const delayBetweenBatches = 1000 // 1 second delay between batches
    const projectCodeCache = {}

    for (let i = 0; i < rawData.length; i += batchSize) {
      const batch = rawData.slice(i, i + batchSize)
      const promises = batch.map((entry) =>
        getARCProjectCode(
          entry,
          vismaAPIUrl,
          bearerToken,
          vismaClientId,
          projectCodeCache
        )
          .then((projectCode) => ({
            entry,
            projectCode,
          }))
          .catch((error) => {
            console.error(error)
            return { entry, projectCode: null }
          })
      )

      const results = await Promise.all(promises)

      for (const { entry, projectCode } of results) {
        const internalClientId = extractInternalClientId(projectCode)
        const currentTime = new Date().toISOString()
        let date = entry.eventDate ?? null
        if (date) {
          date = date + " 00:00:00 UTC"
        }

        const formattedEntry = {
          id: entry.guid ?? null,
          employee_id: parseInt(entry.user?.code, 10) ?? null,
          company: null,
          month: entry.eventDate ?? null,
          date: date,
          hours: entry.quantity || null,
          billable_task: entry.isBillable || false,
          user_id: entry.user?.guid || null,
          name: entry.user?.name || null,
          project_code: projectCode,
          client_id: parseInt(entry.customer?.number, 10) || null,
          internal_client_id: internalClientId?.toString() ?? null,
          client: entry.customer?.name || null,
          project: entry.project?.name || null,
          task_id: null, //TODO
          task: entry.workType?.name ?? null,
          dashboard_client: null,
          inserted_at: currentTime,
        }

        processedData.push(formattedEntry)
      }

      // Introduce delay between batches if not processing the last batch
      if (i + batchSize < rawData.length) {
        await new Promise((resolve) => setTimeout(resolve, delayBetweenBatches))
      }
    }

    return processedData
  } catch (error) {
    console.error("FORMAT DATA FAILED")
    console.error(error)
  }
}

// Fetch code from custom field "ARC Project Code"
const getARCProjectCode = async (
  entry,
  vismaAPIUrl,
  bearerToken,
  vismaClientId,
  projectCodeCache
) => {
  const guid = entry.project?.guid ?? null
  if (!guid) return null

  if (guid in projectCodeCache && projectCodeCache[guid] !== null) {
    console.info("REUSED CACHED VALUE: ", projectCodeCache[guid])
    return projectCodeCache[guid]
  }
  try {
    const response = await axios.get(
      `${vismaAPIUrl}/projects/${guid}/customvalues`,
      {
        headers: {
          Authorization: `Bearer ${bearerToken}`,
          client_Id: vismaClientId,
        },
      }
    )

    if (response.status === 200 && response?.data) {
      const result = response.data.find(
        (item) => item.customProperty.name === "ARC Project Code"
      )
      if (result && result.value) {
        console.info("FETCHED NEW VALUE: ", result.value)
        projectCodeCache[guid] = result.value
        return result.value
      }
    }
  } catch (error) {
    console.warn(`Project with missing custom field ARC project code: `, guid)
    return null
  }
}

// Extracts first for numbers from project_id to be used as internal_client_id
const extractInternalClientId = (projectId) => {
  if (projectId === null || projectId === undefined) return null

  // Match the pattern "####-###"
  const match = projectId.match(/^(\d{4})-\d{3}$/)
  if (match) {
    return match[1]
  }
  return null
}

/**
 * BIG QUERY
 */

const writeToBigQuery = async (data, bigquery, datasetId, tableId) => {
  try {
    const dataset = bigquery.dataset(datasetId)
    const table = dataset.table(tableId)
    await table.insert(data)
  } catch (error) {
    console.error(error)
  }
}

// Delete all records from current year that have existed in more than 90minutes in BigQuery
// The reason for the 90 minute limit is that BigQuery only guarantees complete availability of data after 90 mins.
// If any data is not available yet the whole deletion request will fail.
const clearBigQueryData = async (bigquery, fullTableID, absenceProjectId) => {
  try {
    const currentYear = new Date().getFullYear()
    const totalEntriesThisYear = await countEntries(
      bigquery,
      fullTableID,
      currentYear,
      absenceProjectId
    )
    if (totalEntriesThisYear === 0) {
      console.info(`No BigQuery data to delete. Ready for Visma sync.`)
      return true
    }

    const entriesToDelete = await shouldDeleteEntries(bigquery, fullTableID)
    if (entriesToDelete > 0) {
      const sqlQuery = `
          DELETE FROM \`${fullTableID}\`
          WHERE id IN (
            SELECT id
            FROM \`${fullTableID}\`
            WHERE EXTRACT(YEAR FROM date) = @currentYear
              AND inserted_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 MINUTE)
              AND project_code != \`${absenceProjectId}\`
          );
        `
      const options = {
        query: sqlQuery,
        params: {
          currentYear: currentYear,
        },
      }

      // Delete entries from BigQuery and capture the job response
      const [job] = await bigquery.createQueryJob(options)

      // Wait for the query to finish
      await job.getQueryResults()

      // Get the job details to find out the number of affected rows
      const [jobDetails] = await job.getMetadata()
      const numDeletedRows = jobDetails.statistics.query.numDmlAffectedRows

      if (numDeletedRows !== entriesToDelete) {
        console.warn(
          `The amount of deleted rows (${numDeletedRows}) did not match the expected rows to delete (${entriesToDelete}).`
        )
        return false
      }
      console.info(
        `FINISHED DELETING OLD BIGQUERY DATA: ${numDeletedRows} rows deleted`
      )
    } else {
      findMostRecentTimestamp(
        bigquery,
        fullTableID,
        currentYear,
        absenceProjectId
      )
      console.info(
        "Not all entries from this year are older than 90 minutes. No data deleted."
      )
      return false
    }
  } catch (error) {
    console.error("FAILED DELETING OLD BIGQUERY DATA")
    console.error(error)
    return false
  }
  return true
}

const shouldDeleteEntries = async (bigquery, fullTableID) => {
  try {
    const currentYear = new Date().getFullYear()
    const totalEntriesThisYear = await countEntries(
      bigquery,
      fullTableID,
      currentYear,
      absenceProjectId
    )
    const totalEntriesThisYearOlderThan90Min = await countEntries(
      bigquery,
      fullTableID,
      currentYear,
      absenceProjectId,
      true
    )

    if (totalEntriesThisYear === totalEntriesThisYearOlderThan90Min) {
      return totalEntriesThisYearOlderThan90Min
    }
  } catch (error) {
    console.error("Failed to determine if entries should be deleted.")
    console.error(error)
    return 0
  }
  return 0
}
const countEntries = async (
  bigquery,
  fullTableID,
  year,
  absenceProjectId,
  includeOlderThan90Min = false
) => {
  try {
    let additionalCondition = ""
    let msg = "Total entries this year: "
    if (includeOlderThan90Min) {
      additionalCondition =
        "AND inserted_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 MINUTE)"
      msg = "Total entries this year older than 90 minutes: "
    }

    const sqlQuery = `
        SELECT COUNT(*) as totalEntries
        FROM \`${fullTableID}\`
        WHERE EXTRACT(YEAR FROM date) = @year
        AND project_code != \`${absenceProjectId}\`
        ${additionalCondition};
      `
    const options = {
      query: sqlQuery,
      params: {
        year: year,
      },
    }

    // Query BigQuery for the count of entries
    const [rows] = await bigquery.query(options)

    if (rows.length > 0) {
      console.info(`${msg}${rows[0].totalEntries}`)
      return rows[0].totalEntries
    } else {
      console.info(`${msg}0`)
      return 0
    }
  } catch (error) {
    console.error("Failed to count entries.")
    console.error(error)
    return 0
  }
}

// This is used to inform about how much time is left until another sync can be performed.
// Data can only be deleted from Bi after 90minutes have passed from the insertion point.
// Therefore as to not be able to add duplicated data to Bi we need to always delete all data from current year before
// we can add new data. This function determines how much time is left before all data is ready to be synced again.
const findMostRecentTimestamp = async (
  bigquery,
  fullTableID,
  year,
  absenceProjectId
) => {
  try {
    const sqlQuery = `
        SELECT MAX(inserted_at) as mostRecent
        FROM \`${fullTableID}\`
        WHERE EXTRACT(YEAR FROM date) = @year;
        AND project_code != \`${absenceProjectId}\`
      `
    const options = {
      query: sqlQuery,
      params: { year: year },
    }

    const [rows] = await bigquery.query(options)
    if (rows.length > 0 && rows[0].mostRecent) {
      const mostRecent = new Date(rows[0].mostRecent.value)
      const timeLeft = new Date(mostRecent.getTime() + 90 * 60000) - new Date()
      const minutesLeft = Math.ceil(timeLeft / 60000)
      console.info(
        `${minutesLeft} minutes left until all BigQuery data for this year is ready for a full sync.`
      )
    } else {
      console.info("No recent entries found for the current year.")
    }
  } catch (error) {
    console.error("Failed to find the most recent timestamp.")
  }
}

module.exports = { ping }
