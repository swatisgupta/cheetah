#ifdef USE_GPU
#include "cuf_perf_macros.h"
#endif
#include <petscversion.h>
#if PETSC_VERSION_LT(3,8,0)
#include <petsc/finclude/petscdef.h>
#else
#include <petsc/finclude/petsc.h>
#endif
!********************************************************
!! XGCa
!!
!! @Version 2.0  07/10/2013
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
!!  R. Hager
!!  EPSI Team
!********************************************************
program xgca
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
  use load_balance_module
  use ptb_3db_module
  use coupling_module
  use bld_module
  use common_ptl_routines, only : memory_cleaning_simple, save_or_load_electron_phase
  use push_module, only : push
  use shift_module, only : shift_sp
  use main_extra, only : mon_start, mon_stop, parse_args, check_point
  use my_mpi, only : my_mpi_init, my_mpi_finalize
  use setup_module, only : setup
  use diag_f0_module, only : diag_f0
  use adios_comm_module
  use sheath_module
#ifdef ADIOS2
  use adios2_comm_module
#endif
#ifdef USE_GPU
  use push_mod_gpu, only : init_push_mod_gpu, pushe_gpu, push_gpu_manager
  use cudafor
  use cuf_util, only : cufchkerr
  use psn_class_gpu, only : H2D_psn_update_begin
  use ptl_module_gpu, only : HtoD_species_update_begin
  use perf_mod, only : t_accel_startf, t_accel_stopf
#endif
#ifdef USE_VEC
  use pushe_vec_mod
#endif
#ifdef USE_CAB
  use kokkos
  use setup_module_cab, only : setup_XGC_cabana, finalize_XGC_cabana
  use chargee_module_cab, only : prep_chargee_cabana, chargee_cabana
  use pushe_module_cab, only : pushe_cabana, copy_for_cabana_pushe, copy_from_cabana_pushe, prep_pushe_cabana
  use ptl_module_cab, only : cabana_allocate_particle_arrays, cabana_deallocate_particle_arrays
  use shift_ie_module_cab, only : prep_for_shift_cabana, shift_ie_cab
#endif
  use resamp_module, only: resample, resamp_rate

  implicit none
  type(grid_type) :: grid
  type(psn_type) :: psn
  type(species_type) :: spall(0:ptl_nsp_max)
#ifdef USE_GPU
  ! sa: this object will manage data transfers for pushe_gpu
  type(push_gpu_manager) :: pushe_manager
  real(8) :: new_gpu_ratio, step_gpu_ratio
  integer :: istep_rebal_index
#endif
  !
  integer :: istep,ierr,ipc,i, counter
  PetscErrorCode :: perr
  integer :: icycle, ncycle
  logical :: diag_on
  integer :: final_istep
  character (len=10) :: ic(0:15)
  integer :: inode1,inode2
  !jyc: identifier for coupling
  integer, parameter :: color = 1
  logical :: file_exists 
  ! MPI initialize
  ! MPMD staging: XGC color=0 and others are non-zero
  call my_mpi_init(color)

  ! Build Information
  if(sml_mype==0) call print_build_info()
  call parse_args
  if (sml_info_only) stop

  call init_perf_monitor()

  if(sml_mype==0) print *, 'call petsc_init'
  call t_startf("PETSC_INIT")
  ! Pass comm to be used as PETSC_COMM_WORLD
  ! Don't use MPI_COMM_WORLD. Instead, use SML_COMM_WORLD for MPMD staging execution
  call petsc_init(sml_comm_world,perr)
  call petsc_perf_init(perr)
  call t_stopf("PETSC_INIT")

  call t_startf("TOTAL")

  call mon_start(INIT_)
  call t_startf("INIT")
  call t_set_prefixf("i:")
  call t_adj_detailf(+1)

  !ADIOS INITIALIZE
  call check_point( 'call adios_init' )
  call t_startf("ADIOS_INIT")
  call adios_comm_init()
  call t_stopf("ADIOS_INIT")

#ifdef ADIOS2
  call check_point( 'call adios2_init' )
  call t_startf("ADIOS2_INIT")
  call adios2_comm_init('adios2cfg.xml')
  call t_stopf("ADIOS2_INIT")
#endif

  ! Initialize this to zero:
  sml_istep=0

  call t_startf("SETUP")
  call setup(grid,psn,spall)
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

  ! load balancing cost tracking initialization
  call initialize_load_balance_cost_tracking

! main loop has been restructured -- FIRST may not required
  call t_startf("FIRST")
  call t_adj_detailf(+1)

  if(sml_restart .and. sml_f0_grid) then
     call set_gvid0_pid_from_f0(grid%nnode)
  else
     ! f0_ratio == -1.D0, so only particles are load balanced
     ! here even if sml_f0_grid_load_balance is .true.
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
     else
        !call f0_init_rest(grid)
     endif
     if (sml_plane_index==0) then
       if (sml_mype==0) print *,"dumping f0_grid"
       call dump_f0_grid(grid)
     endif
  endif

  ! Since the electron collisions are called before the ion
  ! collisions but the snapshot is taken only when the ions are
  ! colliding, we have to take an initial snapshot
  if (col_mode==1 .and. .not. sml_deltaf) then
    call t_startf("COL_SNAPSHOT")
    call col_snapshot(spall(1))
    call t_stopf("COL_SNAPSHOT")
    if (sml_electron_on) then
      call t_startf("COL_SNAPSHOT_E")
      call col_snapshot(spall(0))
      call t_stopf("COL_SNAPSHOT_E")
    endif
  endif

  call t_adj_detailf(-1)
  call t_stopf("FIRST")

  if (mon_flush_freq > 0) call flush_perf_monitor(0)

  if (sml_coupling_on) then
    call coupling_turb_read(grid,psn,0)
  endif

  if (sml_emergency_weight_cleanup) then
    call check_point('--> Correct w2 and remove 3D components of f0_f0g in divertor region')
    if (sml_mype==0) then
      print *,'WARNING: TESTED FOR SPECIFIC DIII-D CASES ONLY!!!'
      print *,'SET SML_EMERGENCY_WEIGHT_CLEANUP=.FALSE. UNLESS YOU KNOW WHAT YOU ARE DOING!!!'
    endif
    call correct_w2(grid,psn,spall(ptl_isp:ptl_nsp))
  endif

#ifdef UNIT_TEST
  call run_unit(grid,psn,spall) ! Runs the specified unit test, then exits the program.
#endif

#ifdef USE_GPU
  ! sa : initialize the data transfer manager for PUSHE_GPU
  if (sml_electron_on) then
    call t_startf("pushe_manager_create")
    call pushe_manager % create(psn, spall(0))
    call t_stopf("pushe_manager_create")
  endif
#endif

#ifdef USE_CAB
  ! Do this last-minute so I'm sure grids etc have been set up already.
  call setup_XGC_cabana(grid,psn,spall(0)%num,sml_veclen)
#endif

  call check_point('- do loop start')
  final_istep = sml_mstep+1
  do istep=1,sml_mstep
     call mon_start(MAIN_LOOP_)
     call t_startf("MAIN_LOOP")
     call t_adj_detailf(+1)

#ifdef USE_CAB
#ifdef _OPENACC
     call t_startf("acc_clear_freelists")
     call acc_clear_freelists()
     call t_stopf("acc_clear_freelists")
#endif
#endif

     sml_istep=istep
     sml_gstep=sml_gstep+1

     ! load balancing part ---------------------------------------------
     if (sml_debug_flag) call check_point('load balance')
     call load_balance(spall)
     call update_poloidal_decomposition(grid,psn,spall)

     if (sml_debug_flag) call check_point('sheath_mem_check')
     if (sml_sheath_mode == 2 .and. spall(0)%maxnum .ne. size(sheath_ptl_widx)) then
       call sheath_ptl_mem_dealloc
       call sheath_ptl_mem_alloc(spall(0)%maxnum)
     endif

     if (sml_coupling_on) then
        call coupling_turb_read(grid,psn,istep)
     endif

     ! particle pushing RK2 loop ---------------------------------------
     do ipc=1, sml_nrk
        call t_startf("IPC_LOOP")
        sml_ipc=ipc

#ifdef USE_CAB
        if(sml_electron_on) call cabana_allocate_particle_arrays(spall(0)%num)
        ! Get electron location prepped for chargee scatter
        if(sml_electron_on .and. .not. (sml_special==4 .or. sml_special==1)) then
!          call prep_chargee_cabana(spall(0))
        endif
#endif

        !* Obtain ion charge density
        if (.not. (sml_special==1 .or. sml_special==4)) then
          call t_startf("CHARGEI")
          if (sml_debug_flag) call check_point('Charge deposition ions')
          call chargei(grid,psn,spall(1))  ! weight updated here
          call t_stopf("CHARGEI")
        endif


        !* Obtain electron charge density

        if(sml_electron_on) then
           call t_startf("ELECTRONS")

           if (sml_deltaf_elec) then
             ! solve poisson equation with n_i(t+dt)
             ! and n_e_adiabatic and store it dpot
             call t_startf("POISSON_E1")
             call t_set_prefixf("pe1:")
             if (sml_debug_flag) call check_point('Poisson mode 2')
             call solve_poisson(grid,psn,2)  ! ignore electron response (non-adiabatic) --> adiabatic solution
             !call get_dpot(grid,psn,ipc)  ! save adiabatic solution, then add correction from previous time step
             call t_unset_prefixf()
             call t_stopf("POISSON_E1")
           endif


           ! Obtain electron charge density
           ! using adiabatic response
           call t_startf("CHARGEE")
           if (sml_debug_flag) call check_point('Charge deposition electrons 1')
!#ifdef USE_CAB
!           call chargee_cabana(grid,psn,spall(0)) ! weight updated here
!#else
           call chargee(grid,psn,spall(0)) ! weight updated here
!#endif
           call t_stopf("CHARGEE")

           ! logical sheath calculation
           if(sml_sheath_mode == 1 .and. sml_sheath_adjust .and. sml_ipc==2) then
             ! Adjust logical sheath
             call sheath_adjust(grid,spall(0),sml_sheath_mode)
           endif
           call t_stopf("ELECTRONS")
        endif

        if (.not. (sml_special==1 .or. sml_special==4)) then
          ! Solve Poisson equation - final field for particle push --------
          call t_startf("POISSON")
          call t_set_prefixf("p:")
          if (sml_debug_flag) call check_point('Poisson mode 1')
          call solve_poisson(grid,psn,1)
          !if(sml_electron_on) call save_dpot(grid,psn,ipc)  ! save the final (non-adiabatic) solution of dpot
          call t_unset_prefixf()
          call t_stopf("POISSON")

          call t_startf("GET_POT_GRAD")
          if (sml_debug_flag) call check_point('Get potential gradient')
          call get_potential_grad(grid,psn)
          call t_stopf("GET_POT_GRAD")
        endif

        if(sml_electron_on) then
        !##############################################################
        ! non-blocking gather_field_info can start here
        ! (+ GPU pushe)
        !##############################################################
        endif

        if (ptb_3db_on .and. ipc==1) then
          ! This needs to be done befor the call to diagnosis but after chargei/e
          call t_startf('PTB_3DB_UPDATE')
          call update_3db(grid,psn)
          call t_stopf('PTB_3DB_UPDATE')
        endif

        ! Time advance of ion phase variables
        call determine_diag_on(istep,ipc,diag_on)
        if(diag_2d_more .and. diag_on) then
           call t_startf("diag_2d_additional")
           call diag_2d_additional(grid,psn,spall(1))
           call t_stopf("diag_2d_additional")
        endif

        ! Push electron particles ----------------------------------
        ! electron subcycling routine
        if(sml_electron_on) then

#ifdef USE_GPU
           ! First, determing if the GPU electron pusher will be running
           ! this step.
           if (ipc==1) then
            ! in this case, we will save the e- phase, so we can just
            ! use the value in spall.
            if (spall(0)%num > 0) then
              pushe_manager % run_this_step = .true.
            else
              pushe_manager % run_this_step = .false.
            endif
          else
            ! otherwise, we need to check ptl_enum_save
            if (ptl_enum_save > 0) then
              pushe_manager % run_this_step = .true.
            else
              pushe_manager % run_this_step = .false.
            endif
          endif

           ! sa : do some allocations and constant copies to prep for pushe_gpu
          if (pushe_manager % run_this_step) then
            call t_startf("pushe_preops_blocking")
            ! Set the cache config for PUSH`E out here, just to avoid a possible
            ! device synchonization while setting it in PUSHE
            ierr = cudaDeviceSetCacheConfig(cudaFuncCachePreferL1)
            call cufchkerr(ierr,  __FILE__, __LINE__)
            call pushe_manager % pre_ops_blocking(psn, spall(0), grid)
            call t_stopf("pushe_preops_blocking")
          endif
#endif

#ifdef USE_CAB
           call prep_pushe_cabana(spall(0))
#endif
#ifndef USE_CAB
           call t_startf("SOL_ELEC_PHASE")
           if (sml_debug_flag) call check_point('Save or load electron phase')
           call save_or_load_electron_phase(spall(0),ipc) ! Also f0 is restored.
           call t_stopf("SOL_ELEC_PHASE")
#endif

#ifdef USE_GPU
          if (pushe_manager % run_this_step) then
            call t_startf("pushe_HtoD_species_upd")
            T_START_BONDED(ptl_up, pushe_manager, pushe_manager % ptl_stream, 2)
            ! sa: electrons should be ready to transfer up to the GPU, so start the async copy
            call HtoD_species_update_begin(spall(0), 1, spall(0)%num, &
                                           pushe_manager % ptl_reshaper, &
                                           pushe_manager % ptl_stream)
            ierr = cudaEventRecord(pushe_manager % ptl_up_event, pushe_manager % ptl_stream)
            call cufchkerr(ierr, __FILE__, __LINE__)
            T_STOP_BONDED(ptl_up, pushe_manager, pushe_manager % ptl_stream, 2)
            call t_stopf("pushe_HtoD_species_upd")
          endif
#endif

           select case(ipc)
           case(1)
              ncycle=sml_ncycle_half
           case(2)
              ncycle=sml_ncycle_half*2
           end select
#ifdef USE_GPU
           if (pushe_manager % run_this_step) then
             call t_startf("pushe_H2D_psn_upd")
             T_START_BONDED(psn_up, pushe_manager, pushe_manager % psn_stream, 3)
             call H2D_psn_update_begin(psn, pushe_manager % psn_reshaper, pushe_manager % psn_stream)
             ierr = cudaEventRecord(pushe_manager % psn_up_event, pushe_manager % psn_stream)
             call cufchkerr(ierr, __FILE__, __LINE__)
             T_STOP_BONDED(psn_up, pushe_manager, pushe_manager % psn_stream, 3)
             call t_stopf("pushe_H2D_psn_upd")
           endif
#endif
           call t_startf("PUSHE")
           if (sml_debug_flag) call check_point('Push electrons')
#ifdef USE_GPU
           if (pushe_manager % run_this_step) then
             call t_startf("pushe_balance_start")
             ! sa: start the counters for the GPU/CPU load balancing
             call pushe_manager % balance_start()
             call t_stopf("pushe_balance_start")
             ! sa: all the launches inside pushe_gpu are non-blocking now. Once the CPU
             ! sa: has finished processing all its particles, it will return without waiting
             ! sa: for the GPU to finish its work.
             call t_startf("pushe_gpu")
             call pushe_gpu(pushe_manager, istep,ncycle,grid,psn,spall(0),diag_on)
             call t_stopf("pushe_gpu")
           endif
#else
#ifdef USE_VEC
           call pushe_vec(ncycle,grid,psn,spall(0),diag_on)
#else
#ifdef USE_CAB
           call copy_for_cabana_pushe(psn,spall(0),ipc)
           call pushe_cabana(ncycle,diag_on)
           sml_epc=1
#else
           call pushe(istep,ncycle,grid,psn,spall(0),diag_on)
#endif
#endif
#endif
           call t_stopf("PUSHE")

        endif


        ! Push ion particles ------------------------------------------
        ! Push-ion is after push-electron for GPU parallelism
        call t_startf("PUSH_I")
        call determine_diag_on(istep,ipc,diag_on)
        if (sml_debug_flag) call check_point('Push ions')
        call push(istep,ipc,grid,psn,spall(1),spall(1)%phase0,spall(1)%ptl,diag_on)
        call t_stopf("PUSH_I")

        ! electron push should be finished before this point ---------
#ifdef USE_CAB
        if (sml_electron_on) then
          ! Put the kokkos fence after the push so it happens concurrently. The
          ! copy back to CPU is small enough it's faster to tack it on here.
          call t_startf("Wait_for_pushe")
          call kokkos_fence()
          call t_stopf("Wait_for_pushe")
          call prep_for_shift_cabana
          call copy_from_cabana_pushe(spall(0))
        endif
#endif

#ifdef USE_GPU
        if (sml_electron_on) then
          !sa : we need to wait on the last event on the pushe_gpu sequence, since right
          !   now the diagnostic isn't updated until the very end.
           if (pushe_manager % run_this_step) then
             call t_startf("synchronize_down_event")
             ierr = cudaEventSynchronize(pushe_manager % ptl_down_event)
             call cufchkerr(ierr, __FILE__, __LINE__)
             call t_stopf("synchronize_down_event")

             call t_startf("pushe_balance_stop")
             call pushe_manager % balance_stop()
             call t_stopf("pushe_balance_stop")

             ! It's possible that some stuff in here depends on sml_gpu_ratio,
             ! so don't do the balancing after its finished
             call t_startf("pushe_postops_blocking")
             call pushe_manager % post_ops_blocking(psn, spall(0), grid)
             call t_stopf("pushe_postops_blocking")
           endif

           if (pushe_manager % run_this_step) then
             ! adjust the GPU/CPU balance
             step_gpu_ratio = pushe_manager % calculate_gpu_ratio(spall(0)%num, sml_gpu_ratio)
             ! set the gpu ratio using a running average, to damp down fluctuations
             istep_rebal_index = sml_nrk * (istep - 1) + ipc
             new_gpu_ratio = (sml_gpu_ratio * (istep_rebal_index - 1)+ step_gpu_ratio) / istep_rebal_index
             new_gpu_ratio = max(new_gpu_ratio, 0D0)
             new_gpu_ratio = min(new_gpu_ratio, 1D0)
             ! For debugging, print every rebalance
              if(sml_mype==0) write(6,1888) istep, ipc, step_gpu_ratio, sml_gpu_ratio,new_gpu_ratio
1888 format('step,ipc,step_gpu_ratio,old_gpu_ratio,new_gpu_ratio ',I5,I5,F10.4,F10.4,F10.4)
#ifdef VALIDATION_RUN
             if(sml_mype==0) write(6,*) "*** Validation build, GPU ratio locked ***"
#else
             sml_gpu_ratio = new_gpu_ratio
#endif
           endif
        endif
#endif

        call t_startf("DIAGNOSIS")
        if (sml_debug_flag) call check_point('Diagnosis')
        call diagnosis(istep,ipc,grid,psn,spall)
        call t_stopf("DIAGNOSIS")


        ! Move ions to their domain
        if (sml_debug_flag) call check_point('Shift ions')
        call t_startf("SHIFT_I")

        call t_startf("MEM_CLEAN_I")
        call memory_cleaning_simple(spall) !## ion and electron sepratately?
        call t_stopf("MEM_CLEAN_I")

        call t_set_prefixf("si:")
        call shift_sp(grid,psn,spall(1))
        call t_unset_prefixf()

        call t_stopf("SHIFT_I")

        if(sml_electron_on) then
           if (sml_debug_flag) call check_point('Shift electrons')
           call t_startf("SHIFT_E")
           call t_set_prefixf("se:")
#ifdef USE_CAB
           call shift_ie_cab(spall(0))
           call cabana_deallocate_particle_arrays
#else
           call shift_sp(grid,psn,spall(0))
#endif

           call t_unset_prefixf()
           call t_stopf("SHIFT_E")
        endif

        call t_stopf("IPC_LOOP")
     enddo  !end ion ipc-loop


     if (ptb_3db_on .and. sml_gstep .ge. ptb_3db_start_time  &
         .and. .not. ptb_3db_force_symmetric_f) then
       ! Switch of axisymmetrization of collisions and neutrals
       sml_symmetric_f=.false.
     endif

     ! update f0 , f  - after shift
     if(sml_f0_grid) then
        call t_startf("F0_GRID")
        ! need to search triangle index -- redundant
        call chargei_search_index(grid,psn,spall(1))
        if(sml_electron_on) call chargee_search_index(grid,psn,spall(0))

        ! coulomb collision, neutral collision, and source - update f0_grid
        ! Check of time step is done in f_source now!
        if (sml_debug_flag) call check_point('f_source')
        call f_source(grid,psn,spall)

        ! update charge density
        call chargei_f0(grid,psn)
        if(sml_electron_on) call chargee_f0(grid,psn)

        if (mod(istep,diag_f2d_period)==0) then
          call t_startf("DIAG_2D_F0_F")
          call diag_2d_f0_f(grid,psn)
          call t_stopf("DIAG_2D_F0_F")
        endif

        if(mod(istep,sml_f_source_period)==0) then
           call diag_f0(istep,grid,psn,1) ! 1 for safety. to avoid compiler bug.
        endif

        ! f0_f is allocated in subroutine f_source in a source step
        ! but is needed for diag_f0 ---> deallocate now
        if (allocated(f0_f)) deallocate(f0_f)

        call t_stopf("F0_GRID")
     endif

     if (sml_debug_flag) call check_point('after f_source')

     ! coulomb collision, neutral collision, source for full-f method
     if(.not. sml_f0_grid) then
        !Calculate f0 for ion-ion collision
        call t_startf("COLLISION")
        call collision(istep,spall(1))
        call t_stopf("COLLISION")

        if(sml_source .and. mod(sml_gstep, src_period)==0) then
           call t_startf("SOURCE")
           call source(sml_gstep, spall(1))
           call t_stopf("SOURCE")
        endif
     endif

     ! With RMP it may be necessary to limit the marker particle density
     if (sml_limit_marker_den) then
       if (sml_debug_flag) call check_point('limit_marker_den')
       call limit_marker_den(grid,psn,spall)
     endif

     if (sml_debug_flag) call check_point('update_load_balance_cost_tracking')
     call update_load_balance_cost_tracking

     ! update current time
     sml_time=sml_time+sml_dt


     if ((sml_resamp_on) .AND. mod(sml_gstep,resamp_rate)==0) then
        call check_point('RESAMPLE START')
        !resample particles, recompute ion, electron densities
        call t_startf("RESAMPLE")
        call resample(grid,spall,psn,f0_inode1,f0_inode2,f0_dsmu,f0_dvp,  &
                      f0_nmu,f0_nvp,f0_vp_max,f0_smu_max)
        ! Clean up memory after resampling
        call memory_cleaning_simple(spall)
        call t_stopf("RESAMPLE")
        call check_point('RESAMPLE END')
     endif



     ! periodic restart dump
     if ((mod(sml_gstep,sml_restart_write_period)==0) ) then ! .or. (istep == final_istep)) then
        if (sml_debug_flag) call check_point('Restart_write')
        call t_adj_detailf(-1)
        call mon_start(RESTART_WRITE_)
        call t_startf("RESTART_WRITE")
        call t_adj_detailf(+1)

        call t_startf("MEM_CLEAN_RSTRT")
        call shift_sp(grid,psn,spall(1))
        if(sml_electron_on) call shift_sp(grid,psn,spall(0))
        call memory_cleaning_simple(spall)
        call t_stopf("MEM_CLEAN_RSTRT")

        call restart_write(grid,spall,psn)
        if(sml_mype==0 .and. sml_neutral) call background_edensity0_output(grid,psn) ! for neutral collision

        call t_adj_detailf(-1)
        call t_stopf("RESTART_WRITE")
        call mon_stop(RESTART_WRITE_)
        call t_adj_detailf(+1)
     endif

     if (sml_coupling_on) then
        if (istep == final_istep-1) call coupling_particle_write(spall)
     endif
     
     call t_adj_detailf(-1)
     call t_stopf("MAIN_LOOP")
     call mon_stop(MAIN_LOOP_,final_istep)

     if (sml_debug_flag) call check_point('after mon_stop main loop')

     ! Timer information
     if (mon_flush_freq > 0) then
        if (mod(istep,mon_flush_freq) .eq. 0) &
             call flush_perf_monitor(istep)
     endif

     if (sml_debug_flag) call check_point('end of main loop')

     INQUIRE(FILE="kill_run", EXIST=file_exists)

     if ( file_exists ) then
        print *, "Termination requested!" 
        if (sml_coupling_on) then
           call coupling_particle_write(spall)
        endif
        exit
     endif 

     if (istep >= final_istep) exit
  enddo !end of main loop

  call t_startf("FINALIZE")
  call t_adj_detailf(+1)

  if(sml_mype==0) then
     close(30)
     close(31)
     close(71)
     close(22)
  endif

  if(.NOT. sml_use_simple00 ) call delete_solver( psn%solver00, perr )

  call adios_comm_finalize()
#ifdef ADIOS2
  call adios2_comm_finalize()
#endif

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

#ifdef USE_CAB
  call finalize_XGC_cabana
#endif

  print *, "Ending program!" 

  ! MPI_finalize
  call my_mpi_finalize

1000 format (8(e10.3))
1001 format (8(f9.3,'%'))
end program xgca
