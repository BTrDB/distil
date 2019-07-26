package distilclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	logging "github.com/op/go-logging"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

var lg *logging.Logger

func init() {
	lg = logging.MustGetLogger("log")
}

type DistilClient struct {
	globalctx         context.Context
	ec                *etcd.Client
	class             string
	pod               string
	lease             etcd.LeaseID
	assignmentChannel chan *Assignment
}

func NewDistilClient(ctx context.Context, etcd_endpoint string, class string, podname string) *DistilClient {
	if etcd_endpoint == "" {
		lg.Fatalf("no etcd endpoint ($ETCD_ENDPOINT)")
	}
	if class == "" {
		lg.Fatalf("no class")
	}
	if podname == "" {
		lg.Fatalf("no pod name ($MY_POD_NAME)")
	}
	var config etcd.Config = etcd.Config{Context: ctx, Endpoints: []string{etcd_endpoint}}
	client, err := etcd.New(config)
	if err != nil {
		lg.Fatalf("etcd connect error: %v", err)
	}
	err = client.Sync(context.Background())
	if err != nil {
		lg.Fatalf("etcd sync error: %v", err)
	}

	return &DistilClient{
		globalctx: ctx,
		ec:        client,
		class:     class,
		pod:       podname,
	}
}

func (ic *DistilClient) Etcd() *etcd.Client {
	return ic.ec
}

//This takes care of all the hard stuff:
// - New assignments get returned
// - Removed assignments get their work context cancelled
// - Lock only removed once LockContext is cancelled
// - Assignments with changed parameters look like
//    - remove
//    - wait for lock context cancel
//    - new with new params
func (ic *DistilClient) Announce() chan *Assignment {
	lr, err := ic.ec.Grant(ic.globalctx, 10)
	if err != nil {
		lg.Fatalf("could not obtain lease grant: %v", err)
	}
	ic.lease = lr.ID
	var keepalive func()
	keepalive = func() {
		ch, err := ic.ec.KeepAlive(ic.globalctx, ic.lease)
		if err != nil {
			lg.Fatalf("count not keep lease alive: %v", err)
		}
		for {
			select {
			case <-ic.globalctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					lg.Warningf("keepalive channel closed")
					go keepalive()
				}
			}
		}
	}
	go keepalive()
	//Place our worker lock
	lockkey := "distil/workers/" + enc(ic.class) + "/" + ic.pod
	txr, err := ic.ec.Txn(context.Background()).
		If(etcd.Compare(etcd.Version(lockkey), "=", 0)).
		Then(etcd.OpPut(lockkey, "LOCK", etcd.WithLease(ic.lease))).
		Commit()
	if err != nil {
		lg.Fatalf("could not announce worker: %v", err)
	}
	if !txr.Succeeded {
		lg.Fatalf("another worker with the same pod name/class?")
	}

	ic.assignmentChannel = make(chan *Assignment, 100)
	go ic.provideAssignments()
	return ic.assignmentChannel
}

type Assignment struct {
	WorkContext       context.Context
	workContextCancel context.CancelFunc

	LockContext       context.Context
	LockContextCancel context.CancelFunc

	Name         string
	Subname      string
	Parameters   map[string]string
	InputStreams []AssignmentStream
	Extra        map[string]string
}

type AssignmentID struct {
	Name    string
	Subname string
}

func enc(s string) string {
	return base64.URLEncoding.EncodeToString([]byte(s))
}
func dec(s string) string {
	rv, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		lg.Fatalf("encountered undecodable b64: %q : %v", s, err)
	}
	return string(rv)
}

type SerializedAssignment struct {
	Worker       string
	InputStreams []AssignmentStream
	Parameters   map[string]string
	Extra        map[string]string
}

type AssignmentStream struct {
	Name string
	UUID []byte
}

func unmarshalAllocation(v []byte) *SerializedAssignment {
	rv := &SerializedAssignment{}
	err := json.Unmarshal(v, rv)
	if err != nil {
		lg.Fatalf("invalid serialized allocation: %v", err)
	}
	return rv
}

type assignmentChange struct {
	IsDelete     bool //if false it's a change
	Name         string
	Subname      string
	Parameters   map[string]string
	InputStreams []AssignmentStream
	Extra        map[string]string
}

type internalAssignment struct {
	Assignment    *Assignment
	ChangeChannel chan assignmentChange
}

func (ic *DistilClient) lockAssignment(iname string, subname string) {
	key := fmt.Sprintf("distil/allocations/lock/%s/%s/%s", enc(ic.class), enc(iname), enc(subname))
	for {
		if ic.globalctx.Err() != nil {
			return
		}
		txr, err := ic.ec.Txn(context.Background()).
			If(etcd.Compare(etcd.Version(key), "=", 0)).
			Then(etcd.OpPut(key, "LOCK", etcd.WithLease(ic.lease))).
			Commit()
		if err != nil {
			lg.Fatalf("could not transact to lock distiller: %v", err)
		}
		if txr.Succeeded {
			return
		}
		lg.Warningf("failed to lock distiller %q/%q, will try again in 4 seconds", iname, subname)
		time.Sleep(4 * time.Second)
	}
}

func (iass *internalAssignment) Run(ic *DistilClient) {
	for ch := range iass.ChangeChannel {
		if ic.globalctx.Err() != nil {
			return
		}
		if iass.Assignment == nil {
			if ch.IsDelete {
				continue
			}
			WC, WCcancel := context.WithCancel(context.Background())
			LC, LCcancel := context.WithCancel(context.Background())
			//This is a creation
			iass.Assignment = &Assignment{
				WorkContext:       WC,
				workContextCancel: WCcancel,
				LockContext:       LC,
				LockContextCancel: LCcancel,
				Name:              ch.Name,
				Subname:           ch.Subname,
				Parameters:        ch.Parameters,
				InputStreams:      ch.InputStreams,
				Extra:             ch.Extra,
			}
			//Lock the assignment. This can take some time
			ic.lockAssignment(ch.Name, ch.Subname)
			ic.assignmentChannel <- iass.Assignment
		} else {
			//This is a change/delete
			if ch.IsDelete {
				iass.Assignment.workContextCancel()
				<-iass.Assignment.LockContext.Done()
				//Delete lock
				key := fmt.Sprintf("distil/allocations/lock/%s/%s/%s", enc(ic.class), enc(ch.Name), enc(ch.Subname))
				_, err := ic.ec.Delete(context.Background(), key)
				if err != nil {
					lg.Fatalf("could not unlock assignment: %v", err)
				}
				iass.Assignment = nil
			} else {
				//This is a change
				iass.Assignment.workContextCancel()
				<-iass.Assignment.LockContext.Done()
				//Keep lock but issue new assignment
				WC, WCcancel := context.WithCancel(context.Background())
				LC, LCcancel := context.WithCancel(context.Background())
				//This is a creation
				iass.Assignment = &Assignment{
					WorkContext:       WC,
					workContextCancel: WCcancel,
					LockContext:       LC,
					LockContextCancel: LCcancel,
					Name:              ch.Name,
					Subname:           ch.Subname,
					Parameters:        ch.Parameters,
					InputStreams:      ch.InputStreams,
					Extra:             ch.Extra,
				}
				ic.assignmentChannel <- iass.Assignment
			}
		}
	}
}

func (ic *DistilClient) provideAssignments() {
	classEncoded := enc(ic.class)

	ourAllocationKey := "distil/allocations/worker/" + classEncoded + "/"
	//given assignments
	givenAssignments := make(map[AssignmentID]*internalAssignment)

	//create watch on assignments
	wc := ic.ec.Watch(context.Background(), ourAllocationKey, etcd.WithPrefix(), etcd.WithPrevKV())
	masterChanges := make(chan assignmentChange, 1000)

	//Watch for changes:
	go func() {
		for chg := range masterChanges {
			if ic.globalctx.Err() != nil {
				return
			}
			ass, ok := givenAssignments[AssignmentID{Name: chg.Name, Subname: chg.Subname}]
			if ok {
				ass.ChangeChannel <- chg
			} else {
				//We need to create the assignment
				iass := internalAssignment{
					ChangeChannel: make(chan assignmentChange, 50),
				}
				givenAssignments[AssignmentID{Name: chg.Name, Subname: chg.Subname}] = &iass
				go iass.Run(ic)
				iass.ChangeChannel <- chg
			}
		}
	}()

	//query assignments and feed into master channel
	rv, err := ic.ec.Get(context.Background(), ourAllocationKey, etcd.WithPrefix())
	if err != nil {
		lg.Fatalf("could not query etcd for assignments: %v", err)
	}
	for _, r := range rv.Kvs {
		k := string(r.Key)
		//expecting
		//distil/allocations/worker/<class>/<distillatename>/<subname>
		parts := strings.SplitN(k, "/", -1)
		iname := dec(parts[4])
		sname := dec(parts[5])
		sass := unmarshalAllocation(r.Value)
		if sass.Worker != ic.pod {
			continue
		}
		//Write this as a change to the master
		masterChanges <- assignmentChange{
			Name:         iname,
			Subname:      sname,
			InputStreams: sass.InputStreams,
			Parameters:   sass.Parameters,
			Extra:        sass.Extra,
		}
	}
	//Start feeding in etcd watch changes
	go ic.processChanges(wc, masterChanges)
}

func (ic *DistilClient) processChanges(wc etcd.WatchChan, changes chan assignmentChange) {
	wchan := func(a assignmentChange) {
		select {
		case changes <- a:
		default:
			lg.Fatalf("OVERFLOWED DISTIL CHANGE CHANNEL")
		}
	}

	for ch := range wc {
		if ic.globalctx.Err() != nil {
			return
		}
		for _, e := range ch.Events {
			//distil/allocations/worker/<class>/<distillatename>/<subname>
			if e.Type == mvccpb.DELETE {
				//We need to check if this used to concern us
				oldsass := unmarshalAllocation(e.PrevKv.Value)
				if oldsass.Worker == ic.pod {
					parts := strings.SplitN(string(e.PrevKv.Key), "/", -1)
					iname := dec(parts[4])
					sname := dec(parts[5])
					wchan(assignmentChange{
						IsDelete: true,
						Name:     iname,
						Subname:  sname,
					})
				}
				continue
			}

			//This is a PUT
			newsass := unmarshalAllocation(e.Kv.Value)
			if newsass.Worker != ic.pod {
				//This doesn't concern us. Did it used to?
				if e.PrevKv != nil {
					oldsass := unmarshalAllocation(e.PrevKv.Value)
					if oldsass.Worker == ic.pod {

						//Delete it
						//distil/allocations/worker/<class>/<distillatename>/<subname>
						parts := strings.SplitN(string(e.PrevKv.Key), "/", -1)
						iname := dec(parts[4])
						sname := dec(parts[5])
						wchan(assignmentChange{
							IsDelete: true,
							Name:     iname,
							Subname:  sname,
						})
					}
				}
			} else {
				//This is for us, it's either a change or a new addition
				//distil/allocations/worker/<class>/<distillatename>/<subname>
				parts := strings.SplitN(string(e.Kv.Key), "/", -1)
				iname := dec(parts[4])
				sname := dec(parts[5])
				wchan(assignmentChange{
					IsDelete:     false,
					Name:         iname,
					Subname:      sname,
					InputStreams: newsass.InputStreams,
					Parameters:   newsass.Parameters,
					Extra:        newsass.Extra,
				})
			}
		}
	}
	lg.Fatalf("etcd watch channel terminated")
}
