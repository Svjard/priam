// Type definitions for priam v0.0.1
// Project: https://github.com/Svjard/priam
// Definitions by: Marc Fisher <http://github.com/Svjard>
// Definitions: https://github.com/DefinitelyTyped/DefinitelyTyped

/// <reference path="typings/index.d.ts" />

declare module "priam" {
  type Callback = Function;
  type ResultCallback = (err: Error, result: types.ResultSet) => void;

  import * as events from "events";
  import * as stream from "stream";
  import _Long = require("long");

  var Orm: OrmStatic;
  //var Model: ModelStatic;
  //var Query: QueryStatic;
  //var Schema: SchemaStatic;

  type LogLevel = 'debug' | 'info' | 'warn' | 'error';

  interface OrmOptions {
    connection: {
      contactPoints: Array<string>,
      keyspace: string
    }
    keyspace?: {
      replication: ReplicationStrategy,
      durableWrites: boolean,
      ensureExists: boolean,
      alter: boolean
    }
    logger?: {
      level: LogLevel,
      queries: boolean
    },
    model?: {
      tableName: string | (name: string): string,
      getterSetterName: string | (name: string): string,
      validatorSanitizerName: string | (operation: string, column: string): string,
      typeSpecificSetterName: string | (operation: string, column: string): string,
      table: {
        ensureExists: boolean,
        recreate: boolean,
        recreateColumn: boolean,
        removeExtra: boolean,
        addMissing: boolean
      }
    },
    userDefinedType?: {
      ensureExists: boolean,
      recreate: boolean,
      changeType: boolean,
      addMissing: boolean
    }
  }

  interface OrmStatic {
    new(options?: OrmOptions): Orm;
  }
}