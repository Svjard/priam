// Libraries
import _ from 'lodash';

export default class WrappedStream {
  /**
   * Stream representation of a Cassandra streaming result set.
   * @param {Model} model The ORM model type
   * @class
   */
  constructor(model) {
    /**
     *
     */
    this.model = model;
    this.stream = null;
    this.on = {};
    this.pipe = null;
  }

  /**
   * Attach an event listener to the stream.
   *
   * @param {string} key The name of the event to listen for.
   * @param {Function} func The callback to execute on the event.
   */
  on(key, func) {
    this.on[key] = () => {
      func.apply(this, arguments);
    };

    if (this.stream) {
      return this.stream.on(key, this.on[key]);
    } else {
      return this;
    }
  }

  /**
   * Move the stream's cursor forward by reading the next set of bytes available.
   */
  read() {
    if (this.stream) {
      return this.stream.read();
    }
    return null;
  }

  /**
   * Pipes the stream's data into a different stream.
   *
   * @param {Stream} dest The destination stream to pipe to
   * @param {Object} options The options as specified at {@link https://nodejs.org/api/stream.html#stream_readable_pipe_destination_options}
   */
  pipe(dest, options) {
    if (this.stream) {
      this.stream.pipe(dest, options);
    } else {
      this.pipe = { dest: dest, options: options };
    }
    return dest;
  }

  /**
   * Copy constructor for setting a stream to a WrappedSteam instance.
   *
   * @param {Stream} stream
   */
  _setStream(stream) {
    // set stream
    this.stream = stream;

    // intercept chunk by overridding .add
    if (!(stream instanceof WrappedStream)) {
      const add = stream.add;
      stream.add = (chunk) => {
        if (chunk && this.model && !(chunk instanceof this.model)) {
          chunk = this.model._newFromQueryRow(chunk);
        }
        add.call(stream, chunk);
      };
    }

    // apply on
    _.each(this.on, (func, key) => {
      stream.on(key, this.on[key]);
    });

    // apply pipe
    if (this.pipe) {
      stream.pipe(this.pipe.dest, this.pipe.options);
    }
  }
};
