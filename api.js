import fetch from "node-fetch";
import "./json/examples.json"
/*await fetch('http://127.0.0.1:5000/api/tweets', {method:"GET"}).then(res => res.json()).then(res => console.log(res));
*/

const postBody =  'examples.json'

const options = {
    method: 'POST',
    body: JSON.stringify(postBody),
    headers: {
        'Content-Type': 'application/json'
    }
}

await fetch('http://127.0.0.1:5000/api/upload', options).then(res => res.json()).then(res => console.log(res));