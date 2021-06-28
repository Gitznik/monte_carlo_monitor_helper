if __name__ == '__main__':
    pass

query_get_warehouse_id = """
    query getUser {
    getUser {
        account {
        warehouses {
            uuid
            connectionType
        }
        }
    }
    }
"""

query_get_existing_monitors = """
        query getAllUserDefinedMonitors($userDefinedMonitorTypes: [String], $first: Int, $cursor: String) {
        getAllUserDefinedMonitorsV2(userDefinedMonitorTypes: $userDefinedMonitorTypes, first: $first, after: $cursor) {
            pageInfo {
            startCursor
            endCursor
            hasNextPage
            hasPreviousPage
            __typename
            }
            edges {
            node {
                __typename
                id
                monitorType
                entities
                customRuleEntities: entities
            }
            __typename
            }
            __typename
        }
        }
        """

query_get_mcons_for_tables = """
    query getTable($dwId: UUID, $fullTableId: String, $mcon: String, $isTimeField: Boolean, $isTextField: Boolean, $isNumericField: Boolean, $cursor: String, $versions: Int = 1, $first: Int = 20) {
    getTable(dwId: $dwId, fullTableId: $fullTableId, mcon: $mcon) {
        id
        mcon
        fullTableId
        versions(first: $versions) {
        edges {
            node {
            fields(first: $first, isTimeField: $isTimeField, isTextField: $isTextField, isNumericField: $isNumericField, after: $cursor) {
                edges {
                node {
                    name
                    fieldType
                    isTimeField
                    __typename
                }
                __typename
                }
                __typename
            }
            __typename
            }
            __typename
        }
        __typename
        }
        __typename
    }
    }
"""

query_create_monitor = """
mutation createMonitor($mcon: String!, $monitorType: String!, $fields: [String], $timeAxisName: String, $timeAxisType: String, $scheduleConfig: ScheduleConfigInput, $whereCondition: String) {
  createMonitor(mcon: $mcon, monitorType: $monitorType, fields: $fields, timeAxisName: $timeAxisName, timeAxisType: $timeAxisType, scheduleConfig: $scheduleConfig, whereCondition: $whereCondition) {
    monitor {
      entities
      fields
      type
      timeAxisFieldName
      timeAxisFieldType
      __typename
    }
    __typename
  }
}
"""