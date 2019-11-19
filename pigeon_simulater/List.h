
//  List class  header 
//  Doubly Linked List 

#include <assert.h>
#include <stdio.h>

template <class DType>                // DType must have assign =
class List
  { public:
      List();                         // empty list;
      List(List &);                   // copy constructor
      void kill();                    //like destructor
      ~List();
	  int  Size();
      bool Empty();
      void SetToHead();
      void SetToTail();
      bool SetToLoc(unsigned int);    // true if succesful
      bool AtHead();
      bool AtTail();
      unsigned int Where();           // loc
      bool Fore();                    // move foreward
      bool Back();                    // move backward
      void Insert(DType &);           // after curr++
      DType *Access();                // curr
      void EnQueue(DType &);          // at head
      void DeQueue();          // remove at tail;
       bool Remove();                  // curr--; false if list empty
      void operator =(List &);        // assignment operator
      void Traverse(void (*)(DType *));
      void RTraverse(void (*)(DType *));
      bool IsIn(DType &);
   /*   int  MIsIn(DType &X) {int keep,val=0;
                             if(Empty()) return 0;
                              keep = loc;
                              SetToHead(); 
                      do if(*Access()==X) {val=loc; break;} while(Fore());
                      SetToHead(); cout << loc<<" "<<keep<<endl;
                    for(; loc<keep;) Fore(); return val; }
   */
  protected:
      struct LNode { DType Data; LNode *Nxt, *Prev;};
      LNode *head, *tail, *curr;
      unsigned int loc, tl,sz;          // for user classes
};


