#include <osv/mmu.hh>

int main(void){
	file_vma *fd=mmu::map_file_mmap("/dev/vblk1", addr_range(start, start+size), flags, perm, start);
	return 0;
}
