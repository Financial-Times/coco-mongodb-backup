package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const TEMPLATE = `{
        "ismaster" : %t,
        "secondary" : %t,
        "hosts" : [
                "%s",
                "%s",
                "%s"
        ],
        "primary" : "%s",
        "me" : "%s"
}`
const PRIMARY = "primary"
const SEC1 = "secondary1"
const SEC2 = "secondary2"

func TestMasterIsNotNodeForBackup(t *testing.T) {
	in := strings.NewReader(fmt.Sprintf(TEMPLATE, true, false, SEC1, SEC2, PRIMARY, PRIMARY, PRIMARY))
	dec := json.NewDecoder(in)
	response := make(map[string]interface{})
	dec.Decode(&response)

	v, info := isNodeForBackup(response)

	assert.False(t, v, "isNodeForBackup")
	assert.Equal(t, PRIMARY, info.primary, "primary")
	assert.Equal(t, PRIMARY, info.node, "node")
	assert.Equal(t, SEC1, info.secondaries[0], "first secondary")
	assert.Equal(t, SEC2, info.secondaries[1], "other secondary")
}

func TestLowestSecondaryIsNodeForBackup(t *testing.T) {
	in := strings.NewReader(fmt.Sprintf(TEMPLATE, false, true, SEC1, PRIMARY, SEC2, PRIMARY, SEC1))
	dec := json.NewDecoder(in)
	response := make(map[string]interface{})
	dec.Decode(&response)

	v, info := isNodeForBackup(response)

	assert.True(t, v, "isNodeForBackup")
	assert.Equal(t, PRIMARY, info.primary, "primary")
	assert.Equal(t, SEC1, info.node, "node")
	assert.Equal(t, SEC1, info.secondaries[0], "first secondary")
	assert.Equal(t, SEC2, info.secondaries[1], "other secondary")
}

func TestOtherSecondaryIsNotNodeForBackup(t *testing.T) {
	in := strings.NewReader(
		fmt.Sprintf(TEMPLATE, false, true, SEC2, PRIMARY, SEC1, PRIMARY, SEC2))
	dec := json.NewDecoder(in)
	response := make(map[string]interface{})
	dec.Decode(&response)

	v, info := isNodeForBackup(response)

	assert.False(t, v, "isNodeForBackup")
	assert.Equal(t, PRIMARY, info.primary, "primary")
	assert.Equal(t, SEC2, info.node, "node")
	assert.Equal(t, SEC1, info.secondaries[0], "first secondary")
	assert.Equal(t, SEC2, info.secondaries[1], "other secondary")
}

func TestNoSecondaries(t *testing.T) {
	in := strings.NewReader(`{
        "ismaster" : true,
        "secondary" : false,
        "hosts" : [
                "primary"
        ],
        "primary" : "primary",
        "me" : "primary"
}`)
	dec := json.NewDecoder(in)
	response := make(map[string]interface{})
	dec.Decode(&response)

	v, info := isNodeForBackup(response)

	assert.False(t, v, "isNodeForBackup")
	assert.Equal(t, PRIMARY, info.primary, "primary")
	assert.Equal(t, PRIMARY, info.node, "node")
	assert.Len(t, info.secondaries, 0, "secondaries")
}
