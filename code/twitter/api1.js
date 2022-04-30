import fetch from "node-fetch";//import the module to send the file to the API

import { createRequire } from "module";
const require = createRequire(import.meta.url);
const file1 = require('../../json/examples.json');

const options = {
        method: 'POST',
        body: JSON.stringify(file1),
        headers: {
            'Content-Type': 'application/json'
        }
    }

    await fetch('https://precariedapp.herokuapp.com/set', options)
