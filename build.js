var fs = require('fs');
var p = require('path');
var minimatch = require('minimatch');
var UglifyJS = require('uglify-js');

const srcDir = './src';
const destDir = './build/';
const startComment = 'type-check';
const endComment = 'end-type-check';

function strip(file, callback) {
  var pattern = new RegExp("([\\t ]*\\/\\* ?" + startComment + " ?\\*\\/)[\\s\\S]*?(\\/\\* ?" + endComment + " ?\\*\\/[\\t ]*\\n?)", 'g');
  var data = fs.readFileSync(file);

  data = new Buffer(String(data).replace(pattern, ''));

  var path = destDir + file.replace('src\/', '');
  ensureDirectoryExistence(path);
  fs.writeFileSync(path, data);
  callback();
}

function patternMatcher(pattern) {
  return function(path, stats) {
    var minimatcher = new minimatch.Minimatch(pattern, {matchBase: true})
    return (!minimatcher.negate || stats.isFile()) && minimatcher.match(path)
  }
}

function toMatcherFunction(ignoreEntry) {
  if (typeof ignoreEntry == 'function') {
    return ignoreEntry
  } else {
    return patternMatcher(ignoreEntry)
  }
}

function ensureDirectoryExistence(filePath) {
  var dirname = p.dirname(filePath);
  if (directoryExists(dirname)) {
    return true;
  }
  ensureDirectoryExistence(dirname);
  fs.mkdirSync(dirname);
}

function directoryExists(path) {
  try {
    return fs.statSync(path).isDirectory();
  }
  catch (err) {
    return false;
  }
}

function readdir(path, ignores, callback) {
  if (typeof ignores == 'function') {
    callback = ignores
    ignores = []
  }
  ignores = ignores.map(toMatcherFunction)

  var list = []

  fs.readdir(path, function(err, files) {
    if (err) {
      return callback(err)
    }

    var pending = files.length
    if (!pending) {
      // we are done, woop woop
      return callback(null, list)
    }

    files.forEach(function(file) {
      var filePath = p.join(path, file)
      fs.stat(filePath, function(_err, stats) {
        if (_err) {
          return callback(_err)
        }

        if (ignores.some(function(matcher) { return matcher(filePath, stats) })) {
          pending -= 1
          if (!pending) {
            return callback(null, list)
          }
          return null
        }

        if (stats.isDirectory()) {
          readdir(filePath, ignores, function(__err, res) {
            if (__err) {
              return callback(__err)
            }

            list = list.concat(res)
            pending -= 1
            if (!pending) {
              return callback(null, list)
            }
          })
        } else {
          list.push(filePath)
          pending -= 1
          if (!pending) {
            return callback(null, list)
          }
        }
      });
    });
  });
}

function processFile(i, filenames) {
  if (i < filenames.length) {
    strip(filenames[i], function(err) {
      processFile(i+1, filenames);
    })
  }
}

ensureDirectoryExistence(destDir);
readdir(srcDir, function (err, files) {
  processFile(0, files);

  readdir(destDir, function(err, fls) {
    var result = UglifyJS.minify(fls, {
      outSourceMap: destDir + 'priam.js.map'
    });
  });
});
