const express = require('express');
const spawn = require("child_process").spawn;

const app = express();
const PORT = 3000;

app.use(express.json());

app.get('/hello', (req, res)=>{
    res.set('Content-Type', 'text/html');
    res.status(200).send("<h1>Hello!</h1>");
});
app.post('/', (req, res)=>{
    //console.log(req.body)
    const {framework,op,data} = req.body;
    const inputArgs = ["Driver",framework,op];
    if(op === "searchWord"){
        inputArgs.push(data);
    }else if(op === "searchPhrase"){
        const args = data.split(" ");
        inputArgs.push(...args)
    }
    console.log(inputArgs);
     let worker = spawn("java", inputArgs);
      worker.stdout.on("data", function (output) {
        console.log(output);
      });
      worker.on("close", function (code, signal) {
        console.log("Our Java Snippet has finished with an exit code of " + code);
      })
      res.send("Processing request");
})

app.listen(PORT, (error) =>{
    if(!error)
        console.log("Server is Successfully Running, and App is listening on port "+ PORT);
    else
        console.log("Error occurred, server can't start", error);
    }
);

