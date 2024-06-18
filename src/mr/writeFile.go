package mr

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func writeFile(fileName string, r io.Reader) (err error) {
	dir, file := filepath.Split(fileName)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, file)
	if err != nil {
		return fmt.Errorf("can not create tempFile:%v", f)
	}
	_, err = io.Copy(f, r)
	if err != nil {
		return fmt.Errorf("can not copy content:%v", f)
	}
	err = os.Rename(f.Name(), fileName)
	if err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", fileName, f.Name(), err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("cannot close file: %v", f.Name())
	}
	_ = os.Remove(f.Name())
	return nil
}
