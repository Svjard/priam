// enables ES6 support
require('babel-core/register')({
  "presets": ["es2015", "stage-0"],
  "plugins": ["transform-decorators-legacy", "transform-object-rest-spread"],
  "ignore": ["node_modules"]
});
