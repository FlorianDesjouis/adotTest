const express = require('express');
const csv = require('csv-parser');
const fs = require('fs');
const dataArray = [];

const app = express();
const bodyParser = require("body-parser");
app.use( bodyParser.json() );   
app.use(bodyParser.urlencoded({ extended: true }));

const PORT = 3000;

app.get('/', function(req, res) {
    res.status(200).send('Hello world');
});

app.post('/interest', function(req, res) {
    let result = req.body;
    let count = 0;
    let count2 = 0;
    dataArray.forEach(data => {
        result.forEach(interest => {
            let dist = distance(data.lat, data.lon, interest.lat, interest.lon);
            if (!data.distToCloser || data.distToCloser > dist) {
                data.distToCloser = dist;
                data.interestName = interest.name;
            }
        })
        count++;
    })
    if(dataArray.length === count) {
        result.forEach(interest => {
            interest.impressions = 0;
            interest.clicks = 0;
            dataArray.forEach(data => {
                if (data.interestName === interest.name) {
                    if (data.event_type === 'imp') {
                        interest.impressions++;
                    } else if (data.event_type === 'click') {
                        interest.clicks++;
                    }
                }
            })
            count2++;
        })
    }
    if (result.length == count2) {
        res.json(result);
    }
})

fs.createReadStream('events.csv')
  .pipe(csv())
  .on('data', (row) => {
    dataArray.push(row);
  })
  .on('end', () => {
    console.log('CSV file successfully processed');
});

app.listen(PORT, function() {
    console.log('Server is running on PORT:',PORT);
});

function distance(lat1, lon1, lat2, lon2, unit) {
	if ((lat1 == lat2) && (lon1 == lon2)) {
		return 0;
	}
	else {
		let radlat1 = Math.PI * lat1/180;
		let radlat2 = Math.PI * lat2/180;
		let theta = lon1-lon2;
		let radtheta = Math.PI * theta/180;
		let dist = Math.sin(radlat1) * Math.sin(radlat2) + Math.cos(radlat1) * Math.cos(radlat2) * Math.cos(radtheta);
		if (dist > 1) {
			dist = 1;
		}
		dist = Math.acos(dist);
		dist = dist * 180/Math.PI;
		dist = dist * 60 * 1.1515;
		if (unit=="K") { dist = dist * 1.609344 }
        if (unit=="N") { dist = dist * 0.8684 }
		return dist;
	}
}