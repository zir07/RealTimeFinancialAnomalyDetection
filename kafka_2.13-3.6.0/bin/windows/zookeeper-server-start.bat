@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

IF [%1] EQU [] (
	echo USAGE: %0 zookeeper.properties
	EXIT /B 1
)

SetLocal
IF ["%KAFKA_LOG4J_OPTS%"] EQU [""] (
	set KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%~dp0..\..\config\log4j.properties
)
IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
    set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M
)
java %KAFKA_HEAP_OPTS% -cp "%~dp0..\..\libs\*" org.apache.zookeeper.server.quorum.QuorumPeerMain %*
EndLocal
