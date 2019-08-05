package tools

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCentre(t *testing.T) {
	assert.Equal(t, Centre(`/**/cbk64211({"a":"1"})`, "{", "}"), `{"a":"1"}`)
	assert.Equal(t, Centre("#[a]#", "[", "["), "")
	assert.Equal(t, Centre("#[a]#", "]", "["), "")
	assert.Equal(t, Centre("#[a]#", "1", "2"), "")
}
