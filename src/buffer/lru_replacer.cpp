//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include<stack>
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages):head_(new dlist),rear_(new dlist),lrumap_(),size_(num_pages),in_size(0) 
{
    head_->next=rear_;
    rear_->prev=head_;
}

LRUReplacer::~LRUReplacer(){
  std::stack<dlist* > dstack;
  head_=head_->next;
  while(head_!=rear_)
     dstack.push(head_);
  while(!dstack.empty())
  {
      delete dstack.top();
      dstack.pop();
  }
};

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool 
{ 
    if(in_size>0)
    {
      dlist* target=rear_->prev;
      *frame_id=target->frame;
      ddelete(target);
      return true;
    }
    
    return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) 
{
   dlist* target=lrumap_[frame_id];
   ddelete(target);
}

void LRUReplacer::Unpin(frame_id_t frame_id) 
{     if(in_size==size_)
        return ;
    //make new node , hash
      if(lrumap_.find(frame_id)!=lrumap_.end())
        return ;
      dlist* newnode=new dlist;
      newnode->frame=frame_id;
      lrumap_[frame_id]=newnode;
    // insert in the head
      insert(head_,newnode);

}
void LRUReplacer::insert(dlist* pos,dlist* target)
{

  pos->next->prev=target;
  target->next=pos->next;
  target->prev=pos;
  pos->next=target;

  //increment size.
  in_size++;
}

void LRUReplacer::ddelete(dlist* target)
{
 target->prev->next=target->next;
 target->next->prev=target->prev;
 
 delete target;
 //decrement size.
 in_size--;
}
auto LRUReplacer::Size() -> size_t { return in_size; }

}  // namespace bustub
