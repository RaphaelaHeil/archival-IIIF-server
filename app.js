let createError = require('http-errors');
let express = require('express');
const config = require('./helpers/Config.js');
let path = require('path');
let cookieParser = require('cookie-parser');
const logger = require('./helpers/Logger.js');
let morgan = require('morgan');
let session = require('express-session');

let indexRouter = require('./routes/index');
let usersRouter = require('./routes/users');
let iiifAuthRouter = require('./routes/iiifAuthApi');
let iiifPresentationApiRouter = require('./routes/iiifPresentationApi');
let iiifImageApiRouter = require('./routes/iiifImageApi');
let importRouter = require('./routes/import');
let fileApiRouter = require('./routes/fileApi');

let app = express();
// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(express.json({limit: '50mb'}));

if (config.env !== 'production')
    app.use(morgan('short', {'stream': logger.stream}));

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.set('trust proxy', 1); // trust first proxy
app.use(session({
    secret: 'keyboard cat',
    resave: false,
    saveUninitialized: true,
    cookie: {
        secure: false,
        maxAge: 600000
    }
}));
app.use('/file-icons', express.static(path.join(__dirname, 'public/file-icons')));
app.use('/public', express.static(config.publicFolder));
app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization");
    next();
});


app.use('/', indexRouter);
app.use('/users', usersRouter);
app.use('/iiif/auth', iiifAuthRouter);
app.use('/', iiifPresentationApiRouter);
app.use('/iiif/image', iiifImageApiRouter);
app.use('/import', importRouter);
app.use('/file', fileApiRouter);


// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

app.listen(config.port);

module.exports = app;