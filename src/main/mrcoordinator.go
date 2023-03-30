package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
/*
pg-being_ernest.txt
pg-dorian_gray.txt
pg-frankenstein.txt
pg-grimm.txt
pg-huckleberry_finn.txt
pg-metamorphosis.txt
pg-sherlock_holmes.txt
pg-tom_sawyer.txt
*/
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	m := mr.MakeCoordinator(os.Args[1:], 10) //传入参数的文件名
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
