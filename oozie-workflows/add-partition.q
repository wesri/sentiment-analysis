-- NOTE: This file is a modified version of the original licensed file.
-- Original file is at https://github.com/cloudera/cdh-twitter-example
--
--  Licensed to the Apache Software Foundation (ASF) under one
--  or more contributor license agreements.  See the NOTICE file
--  distributed with this work for additional information
--  regarding copyright ownership.  The ASF licenses this file
--  to you under the Apache License, Version 2.0 (the
--  "License"); you may not use this file except in compliance
--  with the License.  You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--

ADD JAR ${JSON_SERDE};
ALTER TABLE tweets ADD IF NOT EXISTS PARTITION (datehour = ${DATEHOUR}) LOCATION '${WFINPUT}';
INSERT OVERWRITE DIRECTORY '/tmp/hive-mapred/${DATEHOUR}'
SELECT concat(id,'\t', created_at,'\t', text) from tweets where datehour = ${DATEHOUR} and lang = 'en';
