
// List class  implememntation
// Double Linked List 

#include "List.h"

template <class DType>
List<DType>::List() {head = tail = curr = NULL; loc = tl =sz= 0;}   // empty list;

template <class DType>
List<DType>::List(List &L)
{ if (L.head == NULL) {head = tail = curr = NULL; loc =tl=sz= 0;}    // empty
  else
   { LNode *p = L.head, *q, *r;
     r = new LNode;  assert (r != NULL);
     r -> Data = p -> Data; r -> Prev = NULL; head = r;
     for ( tl = 1, p = p -> Nxt, q = r; p != NULL; p = p -> Nxt, q = r, tl++)
       { r = new LNode;  assert (r != NULL);
         r -> Data = p -> Data; r -> Prev = q; q -> Nxt = r;
        }
      tail = r; r -> Nxt = NULL; loc = L.loc; SetToLoc(loc); 
	  sz = L.sz;
    }
 }

template <class DType>
void List<DType>::kill()
 { LNode *p, *q;
   for (p = head; p != NULL; p = q)  {q = p -> Nxt; delete p;}
   head = tail = curr = NULL; loc = tl = sz = 0;
 }

template <class DType>
List<DType>::~List() {kill();}

template <class DType>
bool List<DType>::Empty() {return (head == NULL);}

template <class DType>
int List<DType>::Size() {return sz;}

template <class DType>
void List<DType>::SetToHead() {curr = head; loc = (head == NULL) ? 0 : 1;}

template <class DType>
void List<DType>::SetToTail() {curr = tail; loc = (head == NULL) ? 0 : tl;}

template <class DType>
bool List<DType>::SetToLoc(unsigned int i)
 { if ((head == NULL) || (i > tl)) return false;
   for ( ; loc < i ; loc++) curr = curr -> Nxt;
   for ( ; loc > i ; loc--) curr = curr -> Prev;
   return true;
 }

template <class DType>
bool List<DType>::AtHead() {return ((curr == NULL)||(curr -> Prev == NULL));}

template <class DType>
bool List<DType>::AtTail() {return ((curr == NULL)||(curr -> Nxt == NULL));}

template <class DType>
unsigned int List<DType>::Where() {return loc;}

template <class DType>
bool List<DType>::Fore()        // move foreward
        { if ((curr == NULL)||(curr -> Nxt == NULL)) return false;
          curr = curr -> Nxt; loc++; return true;}

template <class DType>
bool List<DType>::Back()        // move backward
        { if ((curr == NULL)||(curr -> Prev == NULL)) return false;
          curr = curr -> Prev; loc--; return true;}

template <class DType>  // at curr; curr moved forward
void List<DType>::Insert(DType &D)
 { LNode *p; p = new LNode; assert (p != NULL); p -> Data = D;
   if (head == NULL)
    {head = curr = tail = p; loc =tl=sz=1; p -> Nxt = p -> Prev = NULL;}
   else
    { p -> Nxt = curr -> Nxt; p -> Prev = curr;
      if (curr -> Nxt != NULL) (curr -> Nxt) -> Prev = p;
      else tail = p;
      curr -> Nxt = p; curr = p; loc++; tl++; sz++;
    }
 }

template <class DType>
DType *List<DType>::Access()     // curr
{ if (curr == NULL) return NULL; return &(curr -> Data);}

template <class DType>
void List<DType>::EnQueue(DType &D)     // loc, curr unchanged
 { LNode *p; p = new LNode; assert (p != NULL);
   p -> Data = D; p -> Nxt = head; p -> Prev = NULL;
   if (head == NULL)
    {head = curr = tail = p; loc = tl = sz=1;}
   else
    { head = head -> Prev = p; loc++; tl++; sz++;}
 }

template <class DType>
void List<DType>::DeQueue()  //loc curr changed
{ SetToTail(); Remove(); }

template <class DType>
bool List<DType>::Remove() //curr -- move one forward
 { if ( head == NULL) return false;
   LNode *p = curr;
   if (p -> Prev == NULL) 
   {head = curr = p -> Nxt; loc = (head == NULL)?0:1;}
   else {curr = p -> Prev; curr -> Nxt = p -> Nxt;  loc--;}
   if (p -> Nxt == NULL) tail = p -> Prev;
   else (p -> Nxt) -> Prev = p -> Prev;
   delete p; tl--; sz--;  return true;
}

template <class DType>
void List<DType>::operator =(List &L)
 { kill();      // rest is copied from copy constructor
   if (L.head == NULL) {head = tail = curr = NULL; loc = tl = 0;}    // empty
   else
   { LNode *p = L.head, *q, *r;
     r = new LNode;  assert (r != NULL);
     r -> Data = p -> Data; r -> Prev = NULL; head = r;
     for ( tl = 1, p = p -> Nxt, q = r; p != NULL; p = p -> Nxt, q = r, tl++)
       { r = new LNode;  assert (r != NULL);
         r -> Data = p -> Data; r -> Prev = q; q -> Nxt = r;
       }
      tail = r; r -> Nxt = NULL; loc = L.loc; SetToLoc(loc);
	  sz=L.sz;
    }
 }

template <class DType>
void List<DType>::Traverse(void (*DoIt)(DType *))
        { if (Empty()) return;
          SetToHead();
          do DoIt(Access()); while (Fore());
        }

template <class DType>
void List<DType>::RTraverse(void (*DoIt)(DType *))
        { if (Empty()) return;
          SetToTail();
          do DoIt(Access()); while (Back());
        }

template <class DType>
bool List<DType>::IsIn(DType &X)
      { if (Empty()) return false;
          SetToHead();
          do if (*Access() == X) return true; while (Fore());  // overload ==
          return false;
      }


