const express = require('express');
const apiRoutes = require('./routes/api.js');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());




const PORT = process.env.PORT || 8000;



app.use('/api', apiRoutes);

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
