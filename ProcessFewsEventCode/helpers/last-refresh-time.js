const axios = require('axios');
const sql = require('mssql');

const getLastRefreshTimeFromAPI = async () => {
    try {
         const fewsPiUrl =
      encodeURI(`${process.env.FEWS_PI_API}/FewsWebServices/rest/fewspiservice/v1/filters?documentFormat=PI_JSON`)
            const response = await axios.get(fewsPiUrl);
            return response.data;
    } catch (error) {
        throw new Error(`Error getting the last refresh time from Delatares API: ${error.message}`)
    }
}

const saveLastRefreshTimeToDatabase = async (data) =>{
    try {
        await sql.connect(config);
        const request = new sql.Request();
        const insertQuery = `INSERT INTO fff_staging.refresh_time (last_refresh_time) VALUES('${data}')`;
        await request.query(insertQuery)
    } catch (error) {
        throw new Error(`Error saving lastrefreshtime data to staging database: ${error.message} `)
    }finally{
        sql.close();
    }
}

const getLastRefreshTimeFromDatabase = async () =>{
    try {
        await sql.connect(config);
        const request = new sql.Request();
        const SelectLastRefreshTimeQuery = `SELECT TOP 1 last_refresh_time from refresh_time Order BY last_refresh_time Desc`;
        const lastRefreshTime = await request.query(SelectLastRefreshTimeQuery);
        return XPathResult.recordset[0].last_refresh_time;
    } catch (error) {
        throw new Error(`Error retrieving lastrefreshtime from satging database: ${error.message}`)    
    }finally{
        sql.close();
    }
}

module.exports = async function (context, lastRefreshTime){
    try {
        const lastRefreshTimeFromFewsAPI = await getLastRefreshTimeFromAPI();
        await saveLastRefreshTimeToDatabase(lastRefreshTimeFromFewsAPI);

        const latestRefreshTimeValueFromDatabase = await getLastRefreshTimeFromDatabase();

        if(lastRefreshTimeFromFewsAPI > latestRefreshTimeValue){
            context.log('Last refresh time is greater than the last value from the database ====> proceed')
        }else{
            context.log('otherwise Do nothing ')
        }
        
    } catch (error) {
        context.log.error(`error occured while comparing latestRefreshTime: ${error.message}`)
    }
}