package main

import (
	"context"
	_ "net/http/pprof"

	"github.com/BTrDB/distil"
)

func main() {
	//uu := uuid.NewRandom()
	//
	// uu := uuid.Parse("d305aeb7-0884-41c1-b44f-95d9ad1cd8e6") //uuid.NewRandom()
	// bdb, _ := btrdb.Connect(context.Background(), "127.0.0.1:4410")
	// s, err := bdb.Create(context.Background(), uu, "output/"+time.Now().Format(time.RFC3339), btrdb.OptKV("name", "stream"), nil)
	// if err != nil {
	// 	if btrdb.ToCodedError(err).Code == 429 {
	// 		s = bdb.StreamFromUUID(uu)
	// 	} else {
	// 		panic(err)
	// 	}
	// }
	// _ = s
	// alg := &Alg{}
	// assz := []*distil.Assignment{
	// 	{
	// 		Instance:      "manual",
	// 		ID:            uu,
	// 		Inputs:        []uuid.UUID{uuid.Parse("4439c918-3b08-4950-9eef-8da082507d88")},
	// 		Outputs:       []uuid.UUID{uu},
	// 		ProcessAfter:  math.MinInt64,
	// 		ProcessBefore: math.MaxInt64, //1363469246617053184, //math.MaxInt64,
	// 	},
	// }
	// D, _ := distil.ManualInstantiate(&distil.ManualInstantiationParameters{
	// 	BTrDBEndpoints: []string{"127.0.0.1:4410"},
	// 	EtcdEndpoint:   "127.0.0.1:2379",
	// 	Assignments:    assz,
	// 	Algorithm:      alg,
	// 	Workers:        1,
	// })
	distil.Begin(&Alg{})
}

type Alg struct {
	asn *distil.Assignment
}

func (a *Alg) Config(asn *distil.Assignment) *distil.Config {
	a.asn = asn
	sz := asn.InputStreams()
	if len(sz) == 0 {
		asn.Abort("no input streams?")
	}
	col, err := sz[0].Collection(context.Background())
	if err != nil {
		asn.Abort("could not get collection for input stream: %v", err)
	}
	tags, err := sz[0].Tags(context.Background())
	if err != nil {
		asn.Abort("could not get tags for input stream: %v", err)
	}
	return &distil.Config{
		EntryStage: distil.StageFilterEmptyChunkedRaw(distil.StageRaw(distil.RebasePadSnap(79), a.RawStage)),
		Version:    4,
		Outputs: []distil.OutputStream{
			distil.OutputStream{
				ReferenceName: "out",
				Collection:    col,
				Name:          *tags["name"],
				Unit:          "clone",
			},
		},
	}
}

// func (a *Alg) EntryStage(in *distil.WindowInput, out *distil.WindowOutput) distil.Stage {
// 	//Mark all stages as requiring processing
// 	for i := 0; i < in.NumSamples(); i++ {
// 		out.AddNextStageRange(in.GetSampleRange(i))
// 	}
// 	return distil.StageChunkRaw(distil.StageRaw(nil, a.RawStage))
// }

func (a *Alg) RawStage(in *distil.Input, out *distil.Output) distil.Stage {
	for i := 0; i < in.NumSamples(0); i++ {
		s := in.Get(0, i)
		out.Add(0, s.Time, s.Value*-0.1)
	}
	return distil.StageDone()
}

// func HighLevelFunc(in *WindowInput, out *Output) *Stage {
//   for i := 0; i < in.NumSamples(0); i++ {
//       w := in.Get(0, i)
//       if w.Count > 0 {
//         out.AddRange(in.RangeOf(i))
//       }
//   }
//   if in.PointWidth > 38 {
//     return AlignedWindowStage(in.PointWidth - 6)
//   } else {
//     return RawStage()
//   }
// }
//
// func DataFunc(in *WindowInput, out *Output) *Stage {
//   for i := 0; i < in.NumSamples(0); i++ {
//     out.Add(0, in.Get(0, i))
//   }
//   return Done()
// }
