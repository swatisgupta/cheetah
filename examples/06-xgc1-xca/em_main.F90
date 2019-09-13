#include <petscversion.h>
#if PETSC_VERSION_LT(3,8,0)
#include <petsc/finclude/petscdef.h>
#else
#include <petsc/finclude/petsc.h>
#endif
!********************************************************
!! XGC1
!!
!! @Version 3.0  2/20/2013
!! @Authors:
!!  S. Ku
!!  P. Worley,
!!  E. D'Azevedo,
!!  M. Adams,
!!  G. Park,
!!  E. Yoon,
!!  S.H. Koh
!!  J.H. Seo
!!  S. Klasky
!!  J. Lang
!!  EPSI Team
!********************************************************
program xgc1_3
  use sml_module
  use ptl_module
  use grid_class
  use psn_class
  use diag_module
  use smooth_module
  use pol_decomp_module
  use src_module
  use random_xgc
  use perf_monitor
  use neu_module
  use f0_module
  use col_module
  use bld_module !< Build Information
#ifdef USE_GPU
  use push_mod_gpu, only : init_push_mod_gpu, pushe_gpu
#endif
  use adios_read_mod
  use xgc_interfaces
!  use fixed_point_accelerator
  use xgc_ts_module
  implicit none
  type(grid_type) :: grid
  type(psn_type) :: psn
  type(species_type) :: spall(0:ptl_nsp_max)
  type(xgc_ts) :: ts
  !
  integer :: istep,ipc,i, mstep, ierr
  PetscErrorCode :: perr
  integer :: ihybrid, epc, icycle, ncycle
  logical :: diag_on
  integer :: final_istep
  integer (8) :: idum1(0:1), idum2(0:1), idum3(0:1)
  character (len=10) :: ic(0:15)
  character (len=10) :: arg
  integer :: inode1,inode2, converged
  real (8), allocatable :: E_node(:,:,:)
  integer :: adios_read_method = ADIOS_READ_METHOD_BP
!  type(fpa_state) :: fpav
!  real (kind=8), allocatable :: fpa_vec(:)
  interface
    subroutine mon_stop(i,est_final_step)
      integer, intent(in) :: i
      integer, optional, intent(inout) :: est_final_step
    end subroutine mon_stop

    subroutine push_fluid(grid,psn,ts,ipc,converged,implicit_flag)
      use grid_class
      use psn_class
      use xgc_ts_module
      !use fixed_point_accelerator
      implicit none
      type(grid_type),target :: grid
      type(psn_type), intent(inout), target :: psn
      type(xgc_ts), intent(inout) :: ts
      integer, intent(in) :: ipc
      logical, intent(in) :: implicit_flag
      integer, intent(out) :: converged
      !type(fpa_state), intent(inout) :: fpav
    end subroutine push_fluid

    subroutine push(istep,ipc,grid,psn,sp,phase0,ptl,diag_on,dt_ext)
      use sml_module
      use ptl_module
      use fld_module
      use grid_class
      use psn_class
      use omp_module , only : split_indices
      use perf_monitor
      use eq_module
      implicit none
      integer, intent(in) :: istep, ipc !! RK4 index
      type(grid_type) :: grid
      type(psn_type) :: psn
      type(species_type) :: sp
      real (kind=8) :: phase0(ptl_nphase,sp%maxnum)   ! sp%phase0 -- for speed
      type(ptl_type) ::  ptl(sp%maxnum)    ! sp%phase
      logical, intent(in) :: diag_on
      real (kind=8), optional :: dt_ext
    end subroutine push

  end interface

  ! MPI initialize
  call my_mpi_init

  ! Build Information
  if(sml_mype==0) call print_build_info()

  call getarg(1,arg)
  if (trim(arg) == '--info' .or. trim(arg)=='-i') then
     stop
  endif

  call init_perf_monitor()

  if(sml_mype==0) print *, 'call petsc_init'
  call t_startf("PETSC_INIT")
  call petsc_init(sml_comm_world,perr)
  call petsc_perf_init(perr)
  call t_stopf("PETSC_INIT")
  call t_startf("TOTAL")

  call mon_start(INIT_)
  call t_startf("INIT")
  call t_set_prefixf("i:")
  call t_adj_detailf(+1)

  !ADIOS INITIALIZE
  if(sml_mype==0) print *, 'call adios_init'
  call t_startf("ADIOS_INIT")
  call adios_init('adioscfg.xml'//char(0), sml_comm, ierr)
  if(sml_mype==0) print *, 'use adios reader v2'
  call adios_read_init_method(adios_read_method, sml_comm, 'verbose=3', ierr)


  call t_stopf("ADIOS_INIT")

  call t_startf("SETUP")
  call setup(grid,psn,spall,ts)
  call t_stopf("SETUP")

  !GPU initialize
#ifdef USE_GPU
  call check_point('Init GPU')
  call t_startf("INIT_PUSHMOD_GPU")
  call init_push_mod_gpu( grid )
  call t_stopf("INIT_PUSHMOD_GPU")
#endif

  call t_adj_detailf(-1)
  call t_unset_prefixf()
  call t_stopf("INIT")
  call mon_stop(INIT_)

  if (mon_flush_freq > 0) call flush_perf_monitor(-1)

  ! main loop --------------------------
  call check_point('main loop started')

! main loop has been restructured -- FIRST may not required
  call t_startf("FIRST")
  call t_adj_detailf(+1)

  if(sml_restart .and. sml_f0_grid) then
     call set_gvid0_pid_from_f0(grid%nnode)
  else
     call t_startf("SET_WEIGHTS_F")
     call set_weights(grid,spall)
     call t_stopf("SET_WEIGHTS_F")
  endif

  call t_startf("SHIFT_I_F")
  call t_set_prefixf("sif:")
  call shift_sp(grid,psn,spall(1))
  call t_unset_prefixf()
  call t_stopf("SHIFT_I_F")

  if(sml_electron_on) then
    call t_startf("SHIFT_E_F")
    call t_set_prefixf("sef:")
    call shift_sp(grid,psn,spall(0))
    call t_unset_prefixf()
    call t_stopf("SHIFT_E_F")
  endif

  if(sml_f0_grid) then
     if(.not. sml_restart) then
        inode1=gvid0_pid(sml_plane_mype)
        inode2=gvid0_pid(sml_plane_mype+1)-1
        !print *, sml_mype,']]]',inode1,inode2
        call f0_initialize(grid,inode1,inode2,0,f0_nmu)
        if (sml_plane_index==0) then
          if (sml_mype==0) print *,"dumping f0_grid"
          call dump_f0_grid(grid)
        endif
     else
        call f0_init_rest(grid)
     endif
  endif

  ! first
  !call t_startf("CHARGEI_F")
  call chargei(grid,psn,spall(1))
  if (.not. sml_restart) then
    ! This sets eden_hyb=idensity when ions are on
    call initial_condition(psn%A_par,psn,grid)
  endif
  !call t_stopf("CHARGEI_F")

  call t_adj_detailf(-1)
  call t_stopf("FIRST")

  if (mon_flush_freq > 0) call flush_perf_monitor(0)

  call check_point('- do loop start')
  final_istep = sml_mstep+1
  !
  !if (sml_use_ts_solver .and. sml_nimp .gt. 0) then
  !  allocate(fpa_vec(3*grid%nnode))
  !  call fpa_create(fpav,fpa_vec,sml_nimp)
  !  deallocate(fpa_vec)
  !endif
  !
  ! Initialize smaller than 0
  if (.not. sml_restart) then
    sml_time=0D0
  endif
  if (sml_use_ts_solver) then
    mstep = 1
  else
    mstep = sml_mstep
  endif

#ifdef UNIT_TEST
  call run_unit(grid,psn,spall) ! Runs the specified unit test, then exits the program.
#endif

  do istep=1,mstep
     call mon_start(MAIN_LOOP_)
     call t_startf("MAIN_LOOP")
     call t_adj_detailf(+1)

     if (.not. sml_use_ts_solver) then
       sml_istep=istep
       sml_gstep=sml_gstep+1
     endif

     ! load balancing part ---------------------------------------------
     ! getting total number of particle and load imbalance
     call t_startf("MAIN_LOOP_RED")
     if(sml_electron_on) then
        idum3=spall(0:1)%num
        call mpi_allreduce(idum3,idum1,2,mpi_integer8,mpi_sum,sml_comm,ierr)
        call mpi_allreduce(idum3,idum2,2,mpi_integer8,mpi_max,sml_comm,ierr)
     else
        idum3(1)=spall(1)%num
        call mpi_allreduce(idum3(1),idum1(1),1,mpi_integer8,mpi_sum,sml_comm,ierr)
        call mpi_allreduce(idum3(1),idum2(1),1,mpi_integer8,mpi_max,sml_comm,ierr)
     endif
     call t_stopf("MAIN_LOOP_RED")

     if(sml_mype==0) print *, 'step,ratio,# of ion  ',istep, real(idum2(1))/(real(idum1(1))/sml_totalpe),idum1(1)
     if(sml_electron_on) then
        if(sml_mype==0) print *, 'step,ratio,# of elec ',istep, real(idum2(0))/(real(idum1(0))/sml_totalpe),idum1(0)
     endif

!pw     if( sml_pol_decomp .and. real(idum2(1))/(real(idum1(1))/real(sml_totalpe)) > sml_max_imbalance ) then
      if(.not. sml_electron_on .and.  sml_pol_decomp .and. (real(idum2(1),8) > sml_max_imbalance*real(spall(1)%min_max_num,8)) ) then
!     if (.false.) then
        ! load balance
        call t_startf("SET_WEIGHTS")
        gvid0_pid_old=gvid0_pid
        call set_weights(grid,spall)   ! enabled
        call t_stopf("SET_WEIGHTS")

        if (sml_f0_grid) then
          call t_startf("F0_REDISTRIBUTE")
          call f0_redistribute(grid,0,f0_nmu)
          call t_stopf("F0_REDISTRIBUTE")
        endif

        pol_decomp_new=.true.

     else if (sml_electron_on) then
!pw        if( sml_pol_decomp .and. real(idum2(0))/(real(idum1(0))/real(sml_totalpe)) > sml_max_imbalance ) then
         if( sml_pol_decomp .and. (real(idum2(0),8) > sml_max_imbalance*real(spall(0)%min_max_num,8)) ) then
!        if (.false.) then
           ! load balance
           call t_startf("SET_WEIGHTS")
           gvid0_pid_old=gvid0_pid
           call set_weights(grid,spall)   ! enabled
           call t_stopf("SET_WEIGHTS")

           if (sml_f0_grid) then
             call t_startf("F0_REDISTRIBUTE")
             call f0_redistribute(grid,0,f0_nmu)
             call t_stopf("F0_REDISTRIBUTE")
           endif

           pol_decomp_new=.true.
        else
           pol_decomp_new=.false.
        endif
     else
        pol_decomp_new=.false.
     endif

     if (pol_decomp_new) then
        call t_startf("SHIFT_I_R")
        call t_set_prefixf("sir:")
        call shift_sp(grid,psn,spall(1))
        call t_unset_prefixf()
        call t_stopf("SHIFT_I_R")

        if(sml_electron_on) then
           call t_startf("SHIFT_E_R")
           call t_set_prefixf("ser:")
           call shift_sp(grid,psn,spall(0))
           call t_unset_prefixf()
           call t_stopf("SHIFT_E_R")
        endif
        pol_decomp_new=.false.
     endif

     ! particle pushing RK2 loop ---------------------------------------
     do ipc=1, sml_nrk
        call t_startf("IPC_LOOP")
        sml_ipc=ipc

        !converged=0

        !* Obtain ion charge density
        !if (.not. sml_use_ts_solver) then
          call t_startf("CHARGEI")
          call chargei(grid,psn,spall(1))  ! weight updated here
          call chargee_hyb_mpisum(grid,psn)
          call t_stopf("CHARGEI")
        !else
        if (sml_use_ts_solver) then
          !psn%idensity_old=0D0
          !psn%idensity=0D0
          !psn%ijpar0=0D0
          !psn%ijpar=0D0
          !psn%eden_hyb0=0D0
          !psn%ejpar0=0D0
          !psn%ejpar=0D0
          !psn%a_par0=0D0
          !psn%dpot0=0D0
          sml_istep=0
          if (.not. sml_restart) then
            psn%dpot=0D0
          endif
          call t_startf("TS_SOLVE")
          call ts_solve(ts, sml_dt, psn%eden_hyb, psn%A_par, psn%dpot(:,1), psn%ejpar, perr);
          call t_stopf("TS_SOLVE")
        endif

        if (.not. sml_use_ts_solver) then
          !rh explicit fluid push
          call t_startf('PUSH_FLUID')
          call push_fluid(grid,psn,ts,ipc,converged,.false.)
          call t_stopf('PUSH_FLUID')

          call t_startf("GET_POT_GRAD")
          !call get_potential_grad(grid,psn)
          allocate(E_node(grid%nnode,3,0:1))
          call get_potential_grad_em(grid,psn,psn%dpot,E_node)
          deallocate(E_node)
          call t_stopf("GET_POT_GRAD")

          if (.not. sml_hyb_adiabatic_elec) then
            call t_startf("GET_POT_GRAD_EPARA")
            call get_potential_grad_epara(grid,psn)
            call t_stopf("GET_POT_GRAD_EPARA")
          endif

          ! Push ion particles ------------------------------------------
          ! Push-ion is after push-electron for GPU parallelism
          call t_startf("PUSH_I")
          call determine_diag_on(istep,ipc,diag_on)
          if (sml_hyb_ion_on) then
            !rh Save some time when running w/o ion terms in the fluid eq.
            ihybrid=1 ! for legacy
            call push(istep,ipc,grid,psn,spall(1),spall(1)%phase0,spall(1)%ptl,diag_on)
          endif
          call t_stopf("PUSH_I")

          ! electron push should be finished before this point ---------

          call t_startf("DIAGNOSIS")
          call diagnosis(istep,ipc,grid,psn,spall)  ! most of diagnosis output happens here. Some are in f_source
          call t_stopf("DIAGNOSIS")


          ! Move ions to their domain
          call t_startf("SHIFT_I")

          call t_startf("MEM_CLEAN_I")
          call memory_cleaning_simple(spall) !## ion and electron sepratately?
          call t_stopf("MEM_CLEAN_I")

          call t_set_prefixf("si:")
          call shift_sp(grid,psn,spall(1))
          call t_unset_prefixf()

          call t_stopf("SHIFT_I")
        endif

        call t_stopf("IPC_LOOP")
        !if (converged==1) exit
     enddo  !end ion ipc-loop


     ! update f0 , f  - after shift
     if(sml_f0_grid) then
        call t_startf("F0_GRID")
        ! need to search triangle index -- redundant
        call t_startf("F0_CHARGEI_SEARCH_INDEX")
        call chargei_search_index(grid,psn,spall(1))
        call t_stopf("F0_CHARGEI_SEARCH_INDEX")

        if (sml_electron_on) then
           call t_startf("F0_CHARGEE_SEARCH_INDEX")
           call chargee_search_index(grid,psn,spall(0))
           call t_stopf("F0_CHARGEE_SEARCH_INDEX")
        endif

        !call update_f0_sp(grid,spall(1))      ! moved to f_source  --> after update_w_ion
        !if(sml_electron_on) call update_f0_sp(grid,spall(0))

        ! coulomb collision, neutral collision, and source - update f0_grid
        !rh Check of time step is done in f_source now!
        call t_startf("F_SOURCE")
        call f_source(grid,psn, spall)
        call t_stopf("F_SOURCE")


        ! get toroidal average of f0g
        ! this routine can be located between update_f0_sp and f_source
        ! --> this will give slight axisymmetry for dt
        if(sml_symmetric_f0g) then
           call t_startf("SYMMETRIC_F0G")
           call symmetric_f0g  ! corresponding w1 will be calculated in charge
           call t_stopf("SYMMETRIC_F0G")
        endif


        ! update charge density
        call t_startf("CHARGEI_F0")
        call chargei_f0(grid,psn)
        call t_stopf("CHARGEI_F0")

        if (sml_electron_on) then
           call t_startf("CHARGEE_F0")
           call chargee_f0(grid,psn)
           call t_stopf("CHARGEE_F0")
        endif

        if(mod(istep,sml_f_source_period)==0) then
           call t_startf("RESET_F0")
           call reset_f0_f
           call t_stopf("RESET_F0")
        endif

        call t_stopf("F0_GRID")
     endif

     ! coulomb collision, neutral collision, source for full-f method
     if(.not. sml_f0_grid) then
        !Calculate f0 for ion-ion collision
        call t_startf("COLLISION")
        call collision(istep,spall(1))
        call t_stopf("COLLISION")

        ! ion/electron-neutral collision
        if(sml_neutral.and.(.not. sml_deltaf)) then
           call t_startf("NEUTRAL")
           call neutral_col(istep,grid,psn,spall(1),spall(1))
           call t_stopf("NEUTRAL")
        endif

        if(sml_source .and. mod(sml_gstep, src_period)==0) then
           call t_startf("SOURCE")
           call source(sml_gstep, spall(1))
           call t_stopf("SOURCE")
        endif
     endif


     ! update current time
     if (.not. sml_use_ts_solver) then
       sml_time=sml_time+sml_dt
     endif

     ! periodic restart dump
     if ( ( (mod(sml_gstep,sml_restart_write_period)==0) .or. (istep == final_istep) ) &
          .and. .not. sml_use_ts_solver ) then
        call t_adj_detailf(-1)
        call mon_start(RESTART_WRITE_)
        call t_startf("RESTART_WRITE")
        call t_adj_detailf(+1)

        call t_startf("MEM_CLEAN_RSTRT")
        call shift_sp(grid,psn,spall(1))
        if(sml_electron_on) call shift_sp(grid,psn,spall(0))
        call memory_cleaning_simple(spall)
        call t_stopf("MEM_CLEAN_RSTRT")

        call restart_write(spall,grid, psn)
        if(sml_mype==0 .and. sml_neutral) call background_edensity0_output(grid,psn) ! for neutral collision

        call t_adj_detailf(-1)
        call t_stopf("RESTART_WRITE")
        call mon_stop(RESTART_WRITE_)
        call t_adj_detailf(+1)
     endif

     call t_adj_detailf(-1)
     call t_stopf("MAIN_LOOP")
     call mon_stop(MAIN_LOOP_,final_istep)

     ! Timer information
     if (mon_flush_freq > 0 .and. .not. sml_use_ts_solver) then
        if (mod(istep,mon_flush_freq) .eq. 0) &
             call flush_perf_monitor(istep)
     endif

     !if (sml_use_ts_solver .and. sml_nimp .gt. 0) then
     !  call fpa_restart(fpav)
     !endif

     if (istep >= final_istep) exit
  enddo !end of main loop

  !if (sml_use_ts_solver .and. sml_nimp .gt. 0) then
  !  call fpa_destroy(fpav)
  !endif

  call t_startf("FINALIZE")
  call t_adj_detailf(+1)

  if(sml_mype==0) then
     close(30)
     close(31)
     close(71)
     close(22)
  endif
  if (sml_use_ts_solver) then
     call ts_delete(ts,perr);
  else
     call KSPDestroy(psn%kspmass,perr)
     call delete_solver( psn%solver00, perr )
  end if
  call adios_finalize(sml_mype,ierr)
  call adios_read_finalize_method (adios_read_method, ierr)

  ! free memories
  call smooth_pol_delete(smooth0L)
  call smooth_pol_delete(smoothH)
  call smooth_pol_delete(smoothdiag)
  call smooth_r_delete(smooth_r1)
  ! free particle memories
  ! ----??

  ! free solver memories
  ! ----??

  !
  call finalize_pol_decomp

  call t_adj_detailf(-1)
  call t_stopf("FINALIZE")

  call t_stopf("TOTAL")

  call finish_perf_monitor(perr)
  ! MPI_finalize
  call my_mpi_finalize

!1000 format (8(e10.3))
!1001 format (8(f9.3,'%'))
end program xgc1_3

#include "main_extra.F90"



